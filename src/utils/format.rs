/// Format bytes into human-readable format
///
/// # Examples
/// ```
/// use rs_block_data_scanner::utils::format::format_size_bytes;
///
/// assert_eq!(format_size_bytes(0), "0 B");
/// assert_eq!(format_size_bytes(1024), "1.00 KB");
/// assert_eq!(format_size_bytes(1048576), "1.00 MB");
/// ```
pub fn format_size_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let bytes_f = bytes as f64;
    let exp = (bytes_f.ln() / 1024_f64.ln()).floor() as usize;
    let exp = exp.min(UNITS.len() - 1);

    let size = bytes_f / 1024_f64.powi(exp as i32);

    format!("{:.2} {}", size, UNITS[exp])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_size_bytes() {
        assert_eq!(format_size_bytes(0), "0 B");
        assert_eq!(format_size_bytes(1), "1.00 B");
        assert_eq!(format_size_bytes(1024), "1.00 KB");
        assert_eq!(format_size_bytes(1048576), "1.00 MB");
        assert_eq!(format_size_bytes(1073741824), "1.00 GB");
        assert_eq!(format_size_bytes(1536), "1.50 KB");
    }
}
