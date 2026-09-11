use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A logger that writes CSV lines
#[derive(Clone)]
pub struct CsvLogger {
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl CsvLogger {
    /// Create or append to a CSV file
    pub fn new(path: &str) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("Unable to open CSV log file");

        let mut writer = BufWriter::new(file);

        // Write header only if file is empty
        let metadata = std::fs::metadata(path).unwrap();
        if metadata.len() == 0 {
            writeln!(writer, "ts_ms,event,key,hit,latency_us,ttl_remaining_ms").unwrap();
            writer.flush().unwrap();
        }

        CsvLogger {
            writer: Arc::new(Mutex::new(writer)),
        }
    }

    pub fn log(
        &self,
        event: &str,
        key: &str,
        hit: bool,
        latency: Duration,
        ttl_remaining: Option<Duration>,
    ) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let line = format!(
            "{},{},{},{},{},{}\n",
            ts,
            event,
            key,
            if hit { 1 } else { 0 },
            latency.as_micros(),
            ttl_remaining
                .map(|d| d.as_millis().to_string())
                .unwrap_or("".into())
        );

        let mut w = self.writer.lock().unwrap();
        w.write_all(line.as_bytes()).unwrap();
        w.flush().unwrap();
    }
}
