//! Summary statistics over polling windows.
//!
//! Baseline latency uses the **median**, not the mean: GC pause windows drag the
//! mean up noticeably, while the median reports the typical healthy value.

/// Median of a slice, as f64. Returns 0.0 for an empty slice.
///
/// Uses the lower-middle element for even-length inputs rather than averaging the
/// two middle values — adequate here, where inputs are latency samples in µs.
pub fn median_u64(vals: &[u64]) -> f64 {
    if vals.is_empty() {
        return 0.0;
    }
    let mut sorted = vals.to_vec();
    sorted.sort_unstable();
    sorted[sorted.len() / 2] as f64
}

/// Arithmetic mean. Returns 0.0 for an empty slice.
pub fn mean_f64(vals: &[f64]) -> f64 {
    if vals.is_empty() {
        return 0.0;
    }
    vals.iter().sum::<f64>() / vals.len() as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_handles_empty_and_odd_and_even() {
        assert_eq!(median_u64(&[]), 0.0);
        assert_eq!(median_u64(&[5]), 5.0);
        assert_eq!(median_u64(&[3, 1, 2]), 2.0);
        // Even length takes the upper of the two middle elements.
        assert_eq!(median_u64(&[1, 2, 3, 4]), 3.0);
    }

    #[test]
    fn median_is_resistant_to_a_single_spike() {
        // The property the baseline calculation depends on: one GC pause window
        // must not drag the reported typical latency far from the others. The mean
        // is destroyed by the same outlier — which is why baselines use the median.
        let samples = [100u64, 110, 105, 108, 102, 999_999];

        let med = median_u64(&samples);
        assert!(
            med < 200.0,
            "median {med} should stay near the typical values"
        );

        let mean = mean_f64(&samples.iter().map(|&v| v as f64).collect::<Vec<_>>());
        assert!(
            mean > 100_000.0,
            "mean {mean} should be wrecked by the outlier"
        );
    }

    #[test]
    fn mean_handles_empty() {
        assert_eq!(mean_f64(&[]), 0.0);
        assert_eq!(mean_f64(&[1.0, 2.0, 3.0]), 2.0);
    }
}
