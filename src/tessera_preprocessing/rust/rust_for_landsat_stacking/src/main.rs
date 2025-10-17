// src/main.rs
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, Datelike};
use env_logger::{Builder, Env};
use glob::glob;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use log::{info, warn, error};
use memmap2::MmapOptions;
use ndarray::Array3;
use rayon::ThreadPoolBuilder;
use regex::Regex;
use std::panic::AssertUnwindSafe;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use structopt::StructOpt;
use tiff::decoder::{Decoder, DecodingResult, Limits};

#[derive(Debug, StructOpt)]
#[structopt(name = "landsat_stack", about = "Process Landsat data for a single tile")]
struct Opt {
    /// Input directory (where TIFF files are)
    #[structopt(short = "i", long = "input-dir", parse(from_os_str))]
    input_dir: PathBuf,

    /// Output directory (where processed NPY files will go)
    #[structopt(short = "o", long = "output-dir", parse(from_os_str))]
    output_dir: PathBuf,
    
    /// Number of parallel processes to use
    #[structopt(short = "p", long = "parallel", default_value = "8")]
    parallel: usize,
    
    /// Downsampling rate (e.g., 10 means take every 10th pixel)
    #[structopt(short = "r", long = "rate", default_value = "10")]
    rate: usize,
}

// Structure to hold metadata about a TIFF file
#[derive(Debug, Clone)]
struct TiffMetadata {
    path: PathBuf,
    date: NaiveDate,
    band: String,
    doy: u16,
}

// Structure to hold a complete set of bands for a date
#[derive(Debug, Clone)]
struct BandSet {
    date: NaiveDate,
    doy: u16,
    blue_path: Option<PathBuf>,
    green_path: Option<PathBuf>,
    red_path: Option<PathBuf>,
    nir08_path: Option<PathBuf>,
    swir16_path: Option<PathBuf>,
    swir22_path: Option<PathBuf>,
    qa_path: Option<PathBuf>,
}

// Structure to hold successfully processed data
#[derive(Debug, Clone)]
struct ProcessedBandSet {
    doy: u16,
    bands_data: Vec<i16>,  // flattened array of shape [height, width, 6]
    mask_data: Vec<u8>,    // flattened array of shape [height, width]
}

fn main() -> Result<()> {
    // Initialize env_logger
    let mut builder = Builder::from_env(Env::default().default_filter_or("info"));
    builder.format_timestamp_secs();
    builder.init();

    let opt = Opt::from_args();

    info!("Starting landsat_stack with {} parallel processes...", opt.parallel);
    info!("Input dir: {:?}", opt.input_dir);
    info!("Output dir: {:?}", opt.output_dir);
    info!("Downsampling rate: {}", opt.rate);

    // Set up Rayon thread pool with specified number of threads
    ThreadPoolBuilder::new()
        .num_threads(opt.parallel)
        .build_global()?;
    
    info!("Initialized thread pool with {} threads", opt.parallel);

    // Create output directory if it doesn't exist
    fs::create_dir_all(&opt.output_dir)?;

    // Find all TIFF files and extract metadata
    info!("Searching for TIFF files in input directory...");
    let tiff_files = find_tiff_files(&opt.input_dir)?;
    info!("Found {} TIFF files", tiff_files.len());

    // Get image dimensions - either from first file or use defaults if no files
    let (height, width) = if tiff_files.is_empty() {
        // Default dimensions if no files found
        let default_h = 525 / opt.rate;  // Landsat standard height for 10m resolution
        let default_w = 600 / opt.rate;  // Landsat standard width for 10m resolution
        warn!("No TIFF files found. Using default downsampled dimensions: H={}, W={}", default_h, default_w);
        (default_h, default_w)
    } else {
        // Try to get dimensions from first few files until we find a valid one
        let mut dimensions = None;
        for file in &tiff_files[..tiff_files.len().min(10)] {
            match get_tiff_dimensions_downsampled(&file.path, opt.rate) {
                Ok((h, w)) => {
                    info!("Got dimensions from {:?}", file.path);
                    info!("Original TIFF dimensions before downsampling: height={}, width={}", h * opt.rate, w * opt.rate);
                    info!("Downsampled TIFF dimensions: height={}, width={}, bands=6", h, w);
                    dimensions = Some((h, w));
                    break;
                }
                Err(e) => {
                    warn!("Failed to get dimensions from {:?}: {}", file.path, e);
                }
            }
        }
        
        match dimensions {
            Some(dims) => dims,
            None => {
                return Err(anyhow!("Could not get dimensions from any TIFF file"));
            }
        }
    };

    // Group files by date and organize bands
    let band_sets = group_and_organize_bands(tiff_files)?;
    
    info!("Organized files: {} date sets", band_sets.len());

    // Set up multi-progress
    let mp = MultiProgress::new();

    // Process all dates
    if !band_sets.is_empty() {
        info!("Processing {} Landsat dates...", band_sets.len());
        let output_dir = opt.output_dir.clone();
        let sets_clone = band_sets.clone();
        let mp_clone = mp.clone();
        let rate = opt.rate;
        
        process_band_sets(&sets_clone, &output_dir, height, width, mp_clone, rate)?;
    } else {
        warn!("No valid band sets found. Creating empty output files.");
        create_empty_files(&opt.output_dir, height, width)?;
    }
    
    // Clear the progress bars when done
    mp.clear()?;

    info!("All done! Output in {:?}", opt.output_dir);
    Ok(())
}

/// Find all TIFF files in the directory and extract metadata
fn find_tiff_files(dir: &Path) -> Result<Vec<TiffMetadata>> {
    // Look for band subdirectories
    let band_dirs = ["blue", "green", "red", "nir", "swir16", "swir22", "qa"];
    let mut all_files = Vec::new();
    
    info!("Searching for TIFF files in band subdirectories...");
    
    for band in &band_dirs {
        let band_dir = dir.join(band);
        if band_dir.exists() && band_dir.is_dir() {
            let pattern = band_dir.join("*.tiff").to_string_lossy().to_string();
            info!("Searching in {}: {}", band, pattern);
            
            let entries: Vec<_> = glob(&pattern)?.collect::<Result<Vec<_>, _>>()?;
            info!("Found {} TIFF files in {} directory", entries.len(), band);
            
            for path in entries {
                if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                    // Extract date from filename (e.g., 2024-01-25_mosaic.tiff)
                    let re = Regex::new(r"(\d{4}-\d{2}-\d{2})_.*\.tiff$")?;
                    if let Some(caps) = re.captures(filename) {
                        let date_str = &caps[1];
                        
                        match NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                            Ok(date) => {
                                let doy = date.ordinal() as u16;
                                let meta = TiffMetadata {
                                    path: path.clone(),
                                    date,
                                    band: band.to_string(),
                                    doy,
                                };
                                all_files.push(meta);
                            },
                            Err(e) => {
                                warn!("Failed to parse date from {}: {}", filename, e);
                            }
                        }
                    } else {
                        warn!("Filename does not match expected pattern: {}", filename);
                    }
                }
            }
        }
    }
    
    info!("Found {} total TIFF files across all band directories", all_files.len());
    
    // Sort metadata by date
    all_files.sort_by(|a, b| a.date.cmp(&b.date));
    
    info!("Successfully processed {} valid TIFF files", all_files.len());
    Ok(all_files)
}

/// Group files by date and organize into band sets
fn group_and_organize_bands(files: Vec<TiffMetadata>) -> Result<Vec<BandSet>> {
    // Group files by date
    let mut date_map: HashMap<NaiveDate, Vec<TiffMetadata>> = HashMap::new();
    
    for file in files {
        date_map.entry(file.date).or_insert_with(Vec::new).push(file);
    }
    
    // Convert to band sets
    let mut band_sets = Vec::new();
    
    for (date, files) in date_map {
        let doy = date.ordinal() as u16;
        let mut band_set = BandSet {
            date,
            doy,
            blue_path: None,
            green_path: None,
            red_path: None,
            nir08_path: None,
            swir16_path: None,
            swir22_path: None,
            qa_path: None,
        };
        
        // Organize files by band
        for file in files {
            match file.band.as_str() {
                "blue" => band_set.blue_path = Some(file.path),
                "green" => band_set.green_path = Some(file.path),
                "red" => band_set.red_path = Some(file.path),
                "nir" => band_set.nir08_path = Some(file.path),  // Map nir to nir08
                "swir16" => band_set.swir16_path = Some(file.path),
                "swir22" => band_set.swir22_path = Some(file.path),
                "qa" => band_set.qa_path = Some(file.path),
                _ => warn!("Unknown band type: {}", file.band),
            }
        }
        
        // Check if we have all required bands (excluding QA which is optional)
        let has_all_bands = band_set.blue_path.is_some() &&
                           band_set.green_path.is_some() &&
                           band_set.red_path.is_some() &&
                           band_set.nir08_path.is_some() &&
                           band_set.swir16_path.is_some() &&
                           band_set.swir22_path.is_some();
        
        if has_all_bands {
            band_sets.push(band_set);
        } else {
            warn!("Skipping date {} due to missing bands", date);
        }
    }
    
    // Sort band sets by date
    band_sets.sort_by(|a, b| a.date.cmp(&b.date));
    
    Ok(band_sets)
}

/// Safe wrapper for processing a single file
fn process_single_file_safe(path: &Path, rate: usize) -> Result<Array3<i16>> {
    // Triple-wrapped panic catching for maximum safety
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<Array3<i16>> {
        let inner_result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<Array3<i16>> {
            read_tiff_mmap_downsampled(path, rate)
        }));
        
        match inner_result {
            Ok(result) => result,
            Err(_) => Err(anyhow!("Inner panic while processing {:?}", path)),
        }
    }));
    
    match result {
        Ok(result) => result,
        Err(_) => Err(anyhow!("Outer panic while processing {:?}", path)),
    }
}

/// Process band sets
fn process_band_sets(
    sets: &[BandSet], 
    output_dir: &Path, 
    height: usize, 
    width: usize, 
    mp: MultiProgress,
    rate: usize,
) -> Result<()> {
    let num_sets = sets.len();
    let num_bands = 6;  // Landsat has 6 bands (excluding QA)
    
    // Create progress bar
    let style = ProgressStyle::default_bar()
        .template("[{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
        .unwrap()
        .progress_chars("#>-");
    let pb = mp.add(ProgressBar::new(num_sets as u64));
    pb.set_style(style);
    pb.set_message("Processing Landsat dates...");
    
    // Collect successfully processed data
    let processed_data_mutex = Arc::new(Mutex::new(Vec::new()));
    
    // Process sets in parallel chunks
    const CHUNK_SIZE: usize = 3;
    
    let sets_chunks: Vec<_> = sets.chunks(CHUNK_SIZE).collect();
    let total_chunks = sets_chunks.len();
    info!("Split processing into {} chunks", total_chunks);
    
    // Process chunks in parallel
    rayon::scope(|s| {
        for (chunk_idx, chunk) in sets_chunks.iter().enumerate() {
            let processed_data_mutex = Arc::clone(&processed_data_mutex);
            let pb = pb.clone();
            
            s.spawn(move |_| {
                info!("Processing chunk {}/{} with {} sets", chunk_idx + 1, total_chunks, chunk.len());
                
                // Process each set in the chunk
                for set in *chunk {
                    // Wrap entire set processing in panic catching
                    let set_result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<ProcessedBandSet> {
                        // Create buffer for this set
                        let mut bands_data = vec![0i16; height * width * num_bands];
                        let mut mask_data = vec![1u8; height * width];  // Default to valid
                        let mut set_valid = true;
                        
                        // Process each band
                        let band_paths = [
                            ("blue", &set.blue_path),
                            ("green", &set.green_path),
                            ("red", &set.red_path),
                            ("nir08", &set.nir08_path),
                            ("swir16", &set.swir16_path),
                            ("swir22", &set.swir22_path),
                        ];
                        
                        for (band_idx, (band_name, path_opt)) in band_paths.iter().enumerate() {
                            if let Some(path) = path_opt {
                                match process_single_file_safe(path, rate) {
                                    Ok(band_data) => {
                                        // Copy band data to combined buffer
                                        for i in 0..height {
                                            for j in 0..width {
                                                bands_data[i * width * num_bands + j * num_bands + band_idx] = band_data[[0, i, j]];
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        error!("Error reading {} band from {:?}: {}", band_name, path, e);
                                        set_valid = false;
                                        break;
                                    }
                                }
                            } else {
                                warn!("Missing {} band for date {}, using zeros", band_name, set.date);
                                set_valid = false;
                                break;
                            }
                        }
                        
                        // Process QA mask if available
                        if let Some(qa_path) = &set.qa_path {
                            match process_single_file_safe(qa_path, rate) {
                                Ok(qa_data) => {
                                    // Convert QA data to mask (simplified logic)
                                    for i in 0..height {
                                        for j in 0..width {
                                            let qa_val = qa_data[[0, i, j]];
                                            // Simple QA logic: 0 = invalid, everything else = valid
                                            mask_data[i * width + j] = if qa_val == 0 { 0 } else { 1 };
                                        }
                                    }
                                },
                                Err(e) => {
                                    warn!("Error reading QA band from {:?}: {}, using default mask", qa_path, e);
                                    // Keep default mask (all valid)
                                }
                            }
                        }
                        
                        // Return result based on validity
                        if set_valid {
                            Ok(ProcessedBandSet {
                                doy: set.doy,
                                bands_data,
                                mask_data,
                            })
                        } else {
                            Err(anyhow!("Set processing failed for date {}", set.date))
                        }
                    }));
                    
                    // Handle the set processing result
                    match set_result {
                        Ok(Ok(processed)) => {
                            let mut processed_data = processed_data_mutex.lock().unwrap();
                            processed_data.push(processed);
                        },
                        Ok(Err(e)) => {
                            error!("Error processing set for date {}: {}", set.date, e);
                        },
                        Err(_) => {
                            error!("Panic occurred while processing set for date {}. Skipping this timestep.", set.date);
                        }
                    }
                    
                    // Update progress
                    pb.inc(1);
                }
            });
        }
    });
    
    pb.finish_with_message("Processed all Landsat dates");
    
    // Get the processed data
    let mut processed_data = Arc::try_unwrap(processed_data_mutex)
        .expect("Failed to unwrap processed data mutex")
        .into_inner()?;
    
    // Sort by date (DOY)
    processed_data.sort_by(|a, b| a.doy.cmp(&b.doy));
    
    let num_successful = processed_data.len();
    info!("Successfully processed {} out of {} sets", num_successful, num_sets);
    
    if num_successful == 0 {
        warn!("No successful sets. Creating empty output files.");
        create_empty_files(output_dir, height, width)?;
        return Ok(());
    }
    
    // Write the data to NPY file with landsat_ prefix
    let bands_file_path = output_dir.join("landsat_bands.npy");
    let mut bands_file = create_npy_file::<i16, 4>(
        &bands_file_path,
        &[num_successful, height, width, num_bands],
    )?;
    
    // Write bands data
    for processed in &processed_data {
        let slice_bytes = unsafe {
            std::slice::from_raw_parts(
                processed.bands_data.as_ptr() as *const u8,
                processed.bands_data.len() * std::mem::size_of::<i16>(),
            )
        };
        bands_file.write_all(slice_bytes)?;
    }
    
    bands_file.flush()?;
    info!("Created Landsat bands file: {:?}", bands_file_path);
    
    // Write masks data
    let masks_file_path = output_dir.join("landsat_masks.npy");
    let mut masks_file = create_npy_file::<u8, 3>(
        &masks_file_path,
        &[num_successful, height, width],
    )?;
    
    for processed in &processed_data {
        let slice_bytes = unsafe {
            std::slice::from_raw_parts(
                processed.mask_data.as_ptr() as *const u8,
                processed.mask_data.len() * std::mem::size_of::<u8>(),
            )
        };
        masks_file.write_all(slice_bytes)?;
    }
    
    masks_file.flush()?;
    info!("Created Landsat masks file: {:?}", masks_file_path);
    
    // Write DOY file
    let doys: Vec<i16> = processed_data.iter().map(|p| p.doy as i16).collect();
    let doy_file_path = output_dir.join("landsat_doys.npy");
    write_npy_simple::<i16, 1>(&doy_file_path, &[doys.len()], &doys)?;
    info!("Created Landsat DOY file: {:?}", doy_file_path);
    
    Ok(())
}

/// Create empty output files
fn create_empty_files(output_dir: &Path, height: usize, width: usize) -> Result<()> {
    // Create empty bands file with landsat_ prefix
    let bands_file_path = output_dir.join("landsat_bands.npy");
    let mut bands_file = create_npy_file::<i16, 4>(
        &bands_file_path,
        &[0, height, width, 6],
    )?;
    bands_file.flush()?;
    info!("Created empty Landsat bands file: {:?}", bands_file_path);
    
    // Create empty masks file
    let masks_file_path = output_dir.join("landsat_masks.npy");
    let mut masks_file = create_npy_file::<u8, 3>(
        &masks_file_path,
        &[0, height, width],
    )?;
    masks_file.flush()?;
    info!("Created empty Landsat masks file: {:?}", masks_file_path);
    
    // Create empty DOY file
    let doy_file_path = output_dir.join("landsat_doys.npy");
    let empty_doys: Vec<i16> = Vec::new();
    write_npy_simple::<i16, 1>(
        &doy_file_path,
        &[0],
        &empty_doys,
    )?;
    info!("Created empty Landsat DOY file: {:?}", doy_file_path);
    
    Ok(())
}

/// Get the dimensions of a TIFF file after downsampling
fn get_tiff_dimensions_downsampled(path: &Path, rate: usize) -> Result<(usize, usize)> {
    info!("Reading dimensions from: {:?}", path);
    
    // Triple-wrapped panic catching for maximum safety
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<(usize, usize)> {
        let inner_result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<(usize, usize)> {
            let file = match File::open(path) {
                Ok(f) => f,
                Err(e) => return Err(anyhow!("Failed to open {:?}: {}", path, e)),
            };
            
            let mmap = match unsafe { MmapOptions::new().map(&file) } {
                Ok(m) => m,
                Err(e) => return Err(anyhow!("Failed to mmap {:?}: {}", path, e)),
            };
            
            // Wrap decoder operations in another catch_unwind
            let decode_result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<(usize, usize)> {
                let cursor = std::io::Cursor::new(&mmap);
                let decoder_result = Decoder::new(cursor);
                
                let mut decoder = match decoder_result {
                    Ok(d) => d,
                    Err(e) => return Err(anyhow!("Failed to create decoder for {:?}: {}", path, e)),
                };
                
                // Use unlimited limits
                decoder = decoder.with_limits(Limits::unlimited());
                
                match decoder.dimensions() {
                    Ok((width, height)) => {
                        // Calculate downsampled dimensions
                        let ds_height = height as usize / rate;
                        let ds_width = width as usize / rate;
                        Ok((ds_height, ds_width))
                    },
                    Err(e) => Err(anyhow!("Failed to get dimensions from {:?}: {}", path, e)),
                }
            }));
            
            match decode_result {
                Ok(result) => result,
                Err(_) => Err(anyhow!("Decoder panic while getting dimensions from {:?}", path)),
            }
        }));
        
        match inner_result {
            Ok(result) => result,
            Err(_) => Err(anyhow!("Inner panic while getting dimensions from {:?}", path)),
        }
    }));
    
    match result {
        Ok(result) => result,
        Err(_) => Err(anyhow!("Outer panic while reading TIFF dimensions from {:?}", path)),
    }
}

/// Read a TIFF file using memory mapping and return a downsampled ndarray
fn read_tiff_mmap_downsampled(path: &Path, rate: usize) -> Result<Array3<i16>> {
    // Triple-wrapped panic catching to handle all possible panics
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<Array3<i16>> {
        let inner_result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<Array3<i16>> {
            // Try to open file with timeout
            let file = match File::open(path) {
                Ok(f) => f,
                Err(e) => return Err(anyhow!("Failed to open {:?}: {}", path, e)),
            };
            
            // Memory map with error handling
            let mmap = match unsafe { MmapOptions::new().map(&file) } {
                Ok(m) => m,
                Err(e) => return Err(anyhow!("Failed to mmap {:?}: {}", path, e)),
            };
            
            // Wrap decoder operations in another catch_unwind for extra safety
            let decode_result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<Array3<i16>> {
                // Create decoder with explicit error handling
                let cursor = std::io::Cursor::new(&mmap);
                let decoder_result = Decoder::new(cursor);
                
                let mut decoder = match decoder_result {
                    Ok(d) => d,
                    Err(e) => return Err(anyhow!("Failed to create decoder for {:?}: {}", path, e)),
                };
                
                // Use unlimited limits
                decoder = decoder.with_limits(Limits::unlimited());
                
                // Get dimensions with error handling
                let (width, height) = match decoder.dimensions() {
                    Ok(dims) => dims,
                    Err(e) => return Err(anyhow!("Failed to get dimensions from {:?}: {}", path, e)),
                };
                
                // Calculate downsampled dimensions
                let ds_height = height as usize / rate;
                let ds_width = width as usize / rate;
                
                // Read image data with detailed error handling
                let image_result = decoder.read_image();
                
                match image_result {
                    Ok(DecodingResult::I16(data)) => {
                        // Expected data length for 1 band
                        let expected_len = height as usize * width as usize;
                        if data.len() != expected_len {
                            return Err(anyhow!(
                                "Data length mismatch in {:?}: got {}, expected {} for shape (1, {}, {})",
                                path, data.len(), expected_len, height, width
                            ));
                        }
                        
                        // Create a new array with downsampled dimensions
                        let mut arr = Array3::<i16>::zeros((1, ds_height, ds_width));
                        
                        // Perform downsampling by taking every rate-th pixel
                        for i in 0..ds_height {
                            for j in 0..ds_width {
                                let src_i = i * rate;
                                let src_j = j * rate;
                                let idx = src_i * width as usize + src_j;
                                if idx < data.len() {
                                    arr[[0, i, j]] = data[idx];
                                }
                            }
                        }
                        
                        Ok(arr)
                    },
                    Ok(DecodingResult::U16(data)) => {
                        // Convert u16 to i16 if necessary
                        let i16_data: Vec<i16> = data.into_iter().map(|x| x as i16).collect();
                        let expected_len = height as usize * width as usize;
                        if i16_data.len() != expected_len {
                            return Err(anyhow!(
                                "Data length mismatch in {:?}: got {}, expected {} for shape (1, {}, {})",
                                path, i16_data.len(), expected_len, height, width
                            ));
                        }
                        
                        // Create a new array with downsampled dimensions
                        let mut arr = Array3::<i16>::zeros((1, ds_height, ds_width));
                        
                        // Perform downsampling by taking every rate-th pixel
                        for i in 0..ds_height {
                            for j in 0..ds_width {
                                let src_i = i * rate;
                                let src_j = j * rate;
                                let idx = src_i * width as usize + src_j;
                                if idx < i16_data.len() {
                                    arr[[0, i, j]] = i16_data[idx];
                                }
                            }
                        }
                        
                        Ok(arr)
                    },
                    Ok(other) => Err(anyhow!("Unsupported decoding result in {:?}: {:?}", path, other)),
                    Err(e) => Err(anyhow!("Failed to decode image from {:?}: {}", path, e)),
                }
            }));
            
            // Handle inner decode panic catch result
            match decode_result {
                Ok(result) => result,
                Err(_) => Err(anyhow!("Decoder panic while processing {:?}", path)),
            }
        }));
        
        // Handle middle panic catch result
        match inner_result {
            Ok(result) => result,
            Err(_) => Err(anyhow!("Inner panic while processing {:?}", path)),
        }
    }));
    
    // Handle outer panic catch result
    match result {
        Ok(result) => result,
        Err(_) => Err(anyhow!("Outer panic while reading TIFF from {:?}", path)),
    }
}

/* ============= NPY file writing helper functions ============= */

/// Trait for numpy dtype
trait NpyDtype {
    fn npy_dtype() -> &'static str;
}

impl NpyDtype for i16 {
    fn npy_dtype() -> &'static str { "<i2" }
}

impl NpyDtype for u8 {
    fn npy_dtype() -> &'static str { "|u1" }
}

/// Create a new empty NPY file with header and return a buffered writer
fn create_npy_file<T: NpyDtype, const N: usize>(
    path: &Path,
    shape: &[usize; N],
) -> Result<BufWriter<File>> {
    // Open file with 256MB buffer
    let file = File::create(path)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024 * 256, file);

    // Write numpy magic
    writer.write_all(b"\x93NUMPY")?;
    // Version 1.0
    writer.write_all(&[1, 0])?;

    // Create header dictionary
    let dtype = T::npy_dtype();
    let dims: Vec<String> = shape.iter().map(|d| d.to_string()).collect();
    let shape_str = if dims.len() == 1 {
        format!("({},)", dims[0])
    } else {
        format!("({})", dims.join(", "))
    };
    let header_dict = format!(
        "{{'descr': '{}', 'fortran_order': False, 'shape': {}, }}",
        dtype, shape_str
    );

    // Align to 16 bytes (including newline)
    let header_len = header_dict.len() + 1;
    let pad = if header_len % 16 == 0 { 0 } else { 16 - (header_len % 16) };
    let padded_header = format!("{}{}", header_dict, " ".repeat(pad));
    let final_header = format!("{}\n", padded_header);
    let header_bytes = final_header.as_bytes();

    // Write header length as 2-byte little-endian
    let header_len_u16 = header_bytes.len() as u16;
    writer.write_all(&header_len_u16.to_le_bytes())?;
    writer.write_all(header_bytes)?;

    // File pointer now at start of data, return writer
    Ok(writer)
}

/// Write a "small" numpy array all at once
fn write_npy_simple<T: Copy + NpyDtype, const N: usize>(
    path: &Path,
    shape: &[usize; N],
    data: &[T],
) -> Result<()> {
    let expected_len: usize = shape.iter().product();
    if data.len() != expected_len {
        return Err(anyhow!(
            "Data length mismatch: got {}, expected {} for shape {:?}",
            data.len(),
            expected_len,
            shape
        ));
    }

    // Use BufWriter with 1MB buffer for small arrays
    let file = File::create(path)?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);

    // Write magic
    writer.write_all(b"\x93NUMPY")?;
    writer.write_all(&[1, 0])?;

    // Create header
    let dtype = T::npy_dtype();
    let dims: Vec<String> = shape.iter().map(|d| d.to_string()).collect();
    let shape_str = if dims.len() == 1 {
        format!("({},)", dims[0])
    } else {
        format!("({})", dims.join(", "))
    };
    let header_dict = format!(
        "{{'descr': '{}', 'fortran_order': False, 'shape': {}, }}",
        dtype, shape_str
    );

    let header_len = header_dict.len() + 1;
    let pad = if header_len % 16 == 0 { 0 } else { 16 - (header_len % 16) };
    let padded_header = format!("{}{}", header_dict, " ".repeat(pad));
    let final_header = format!("{}\n", padded_header);
    let header_bytes = final_header.as_bytes();
    let header_len_u16 = header_bytes.len() as u16;
    writer.write_all(&header_len_u16.to_le_bytes())?;
    writer.write_all(header_bytes)?;

    // Write data
    let data_bytes = unsafe {
        std::slice::from_raw_parts(
            data.as_ptr() as *const u8,
            data.len() * std::mem::size_of::<T>(),
        )
    };
    writer.write_all(data_bytes)?;
    writer.flush()?;

    Ok(())
}