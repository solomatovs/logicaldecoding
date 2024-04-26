use std::io;
use std::path::Path;
use std::time::SystemTime;
use super::Library;
use super::DymodError;


#[derive(Debug)]
pub struct DymodSource {
  version: usize,
  modified_time: std::time::SystemTime,
  source_path: String,
  lib: Library,
}


fn get_modified_date(file_path: &str) -> Result<SystemTime, DymodError> {
  let metadata = match std::fs::metadata(&file_path) {
    Err(e) => return Err(DymodError::IOError(e, String::from(format!("error getting metadata from {} file", file_path)))),
    Ok(metadata) => metadata,
  };

  let modified_time = match metadata.modified() {
    Err(e) => return Err(DymodError::IOError(e, String::from(format!("failed to get modified time of {} file", file_path)))),
    Ok(x) => x,
  };

  Ok(modified_time)
}

impl DymodSource {
  pub fn reload_needed(&self) -> bool {
    match get_modified_date(&&self.source_path) {
      Ok(modified_time) => modified_time != self.modified_time,
      Err(_) => true,
    }
  }

  pub fn version(&self) -> usize {
    self.version
  }

  pub fn source_file(&self) -> &str {
    &self.source_path
  }

  pub fn create_new_version(&self) -> Result<DymodSource, DymodError> {
    let new_lib = DymodSource::new(&self.source_path,  self.version+1)?;

    Ok(new_lib)
  }

  pub fn get_lib_ref(&self) -> Result<&Library, DymodError> {
    return Ok(&self.lib)
  }

  pub fn new(file_path: &str, version: usize) -> Result<Self, DymodError> {
    if !Path::new(&file_path).exists() {
      let io_error = io::Error::new(io::ErrorKind::NotFound, "File not found");
      
      return Err(DymodError::IOError(io_error, format!("source lib {} not found", &file_path)));
    }

    match unsafe {Library::new(&file_path)} {
      Ok(lib) => {
        Ok(DymodSource {
          modified_time: get_modified_date(&file_path)?,
          version: version,
          source_path: file_path.to_string(),
          lib,
        })
      },
      Err(e) => Err(DymodError::LibloadingError(e)),
    }
  }
}

