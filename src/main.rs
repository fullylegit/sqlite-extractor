#![forbid(unsafe_code)]
use anyhow::Result;
use log::{error, info, warn};
use std::env;
use std::io::ErrorKind;
use tokio::fs::{File, OpenOptions};
use tokio::io::{
    AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWriteExt, BufReader, BufWriter, SeekFrom,
};

/// 128 KiB
///
/// This is on the stack so it can't be too ginormous
const BUF_SIZE: usize = 1024 * 128;
/// Sqlite3 magic bytes
const SQLITE_MAGIC: &[u8] = b"SQLite format 3\0";
/// Sqlite3 header size
const HEADER_LEN: usize = 100;

mod offsets {
    pub const PAGE_SIZE: usize = 16;
    pub const FILE_CHANGE_COUNTER: usize = 24;
    pub const DB_SIZE_IN_PAGES: usize = 28;
    pub const VERSION_VALID_FOR: usize = 92;
}

async fn extract<T>(file: &mut T, size: usize) -> Result<Vec<u8>>
where
    T: AsyncRead,
    T: Unpin,
{
    let mut output = Vec::with_capacity(size);
    let mut buf = [0u8; BUF_SIZE];

    let mut num_written = 0;
    while num_written < size {
        let num_read = file.read(&mut buf).await?;
        let buf = &buf[..num_read.min(size - output.len())];
        output.extend_from_slice(buf);
        num_written += buf.len();
    }

    Ok(output)
}

async fn length<T>(file: &mut T) -> Result<Option<usize>>
where
    T: AsyncRead,
    T: Unpin,
{
    let mut buf = [0u8; HEADER_LEN];
    file.read_exact(&mut buf).await?;

    let page_size = {
        let mut page_size = [0u8; 2];
        page_size.copy_from_slice(&buf[offsets::PAGE_SIZE..offsets::PAGE_SIZE + 2]);
        u16::from_be_bytes(page_size)
    };

    // why overcomplicate things?
    if page_size != 1
        && page_size != 2
        && page_size != 4
        && page_size != 8
        && page_size != 16
        && page_size != 32
        && page_size != 64
        && page_size != 128
        && page_size != 256
        && page_size != 512
        && page_size != 1024
        && page_size != 2048
        && page_size != 4096
        && page_size != 8192
        && page_size != 16384
        && page_size != 32768
    {
        return Ok(None);
    }

    let file_change_counter = {
        let mut file_change_counter = [0u8; 4];
        file_change_counter
            .copy_from_slice(&buf[offsets::FILE_CHANGE_COUNTER..offsets::FILE_CHANGE_COUNTER + 4]);
        u32::from_be_bytes(file_change_counter)
    };

    let db_size_in_pages = {
        let mut db_size_in_pages = [0u8; 4];
        db_size_in_pages
            .copy_from_slice(&buf[offsets::DB_SIZE_IN_PAGES..offsets::DB_SIZE_IN_PAGES + 4]);
        u32::from_be_bytes(db_size_in_pages)
    };

    let version_valid_for = {
        let mut version_valid_for = [0u8; 4];
        version_valid_for
            .copy_from_slice(&buf[offsets::VERSION_VALID_FOR..offsets::VERSION_VALID_FOR + 4]);
        u32::from_be_bytes(version_valid_for)
    };

    if file_change_counter != version_valid_for {
        return Ok(None);
    }

    Ok(Some(db_size_in_pages as usize * page_size as usize))
}

async fn next<T>(file: &mut T) -> Result<Option<usize>>
where
    T: AsyncRead + AsyncSeek,
    T: Unpin,
{
    let mut buf = [0u8; BUF_SIZE];
    let num_read = file.read(&mut buf).await?;
    for offset in 0..num_read {
        let buf = &buf[offset..];
        if buf.starts_with(SQLITE_MAGIC) {
            return Ok(Some(
                file.stream_position().await? as usize - num_read + offset,
            ));
        }
    }
    Ok(None)
}

fn init_logging() {
    env_logger::init_from_env(env_logger::Env::new().filter_or(
        "RUST_LOG",
        format!("{}=info", env!("CARGO_PKG_NAME").replace("-", "_")),
    ))
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();
    let filename = "test_data/sample.dump";
    let mut file = BufReader::new(File::open(filename).await?);
    while let Some(offset) = next(&mut file).await? {
        file.seek(SeekFrom::Start(offset as u64)).await?;
        let len = match length(&mut file).await? {
            Some(len) => len,
            None => {
                warn!(
                    "DB found in {} at offset {} but the length was invalid",
                    filename, offset
                );
                continue;
            }
        };
        file.seek(SeekFrom::Start(offset as u64)).await?;
        match extract(&mut file, len).await {
            Ok(db) => {
                let mut path = std::path::PathBuf::from(filename);
                path.set_file_name(format!(
                    "_{}.extracted",
                    path.file_name()
                        .map(|s| s.to_str())
                        .flatten()
                        .unwrap_or_default()
                ));
                match tokio::fs::create_dir(&path).await {
                    Err(err) if err.kind() != ErrorKind::AlreadyExists => {
                        error!("DB found in {} at offset {} with length {} and was extracted but an error occured creating the output directory {:?}: {:?}", filename, offset, len, path.as_os_str(), err);
                        continue;
                    }
                    Ok(()) | Err(_) => {}
                }
                path.push(format!("{:x}.sqlite", offset));
                let mut output = BufWriter::new(
                    match OpenOptions::new()
                        .create_new(true)
                        .write(true)
                        .open(&path)
                        .await
                    {
                        Ok(file) => file,
                        Err(err) if err.kind() == ErrorKind::AlreadyExists => {
                            warn!("DB found in {} at offset {} with length {} and was extracted but the output file already exists {:?}", filename, offset, len, path.as_os_str());
                            continue;
                        }
                        Err(err) => {
                            error!("DB found in {} at offset {} with length {} and was extracted but an error occured creating the output file {:?}: {:?}", filename, offset, len, path.as_os_str(), err);
                            continue;
                        }
                    },
                );
                match output.write_all(&db).await {
                    Ok(()) => {
                        info!(
                            "DB found in {} at offset {} with length {} and was extracted to {:?}",
                            filename,
                            offset,
                            len,
                            path.as_os_str()
                        );
                    }
                    Err(err) => {
                        error!("DB found in {} at offset {} with length {} and was extracted but an error occured writing the output file {:?}: {:?}", filename, offset, len, path.as_os_str(), err);
                    }
                }
            }
            Err(err) => {
                error!("DB found in {} at offset {} with length {} but an error occured extracting it: {:?}", filename, offset, len, err)
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha512};
    use std::io::Cursor;

    const TEST_DATA: &[u8] = include_bytes!("../test_data/sample.dump");

    #[tokio::test]
    async fn test_search() {
        let expected = 4096;
        let mut data = Cursor::new(TEST_DATA);
        let actual = next(&mut data).await.unwrap().unwrap();
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_length() {
        let expected = 12288;
        let mut data = Cursor::new(TEST_DATA);
        data.set_position(4096);
        let actual = length(&mut data).await.unwrap().unwrap();
        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_extract() {
        let expected = "231f6f87758c97f63eb96d28183bffe640101b9ab1dbc6560f60966ce07e84fe1bd001ac57bda76fbe7f8a9a6d5da07a8ca61ff884c9455f9ec121ace31b7bf6";
        let mut data = Cursor::new(TEST_DATA);
        data.set_position(4096);
        let extracted_data = extract(&mut data, 12288).await.unwrap();
        let actual = format!("{:x}", Sha512::digest(extracted_data));
        assert_eq!(expected, actual);
    }
}
