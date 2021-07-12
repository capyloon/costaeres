/// Scorer based on the frecency algorithm
/// See https://developer.mozilla.org/en-US/docs/Mozilla/Tech/Places/Frecency_algorithm
use chrono::{DateTime, Utc};

static MAX_VISIT_ENTRIES: usize = 10;

#[derive(Clone)]

pub enum VisitPriority {
    Normal,
    High,
    VeryHigh,
}

impl VisitPriority {
    // Percentage bonus based on the visit priority.
    pub fn bonus(&self) -> u32 {
        match &self {
            Self::Normal => 100,
            Self::High => 150,
            Self::VeryHigh => 200,
        }
    }
}

#[derive(Clone)]
pub struct VisitEntry {
    timestamp: DateTime<Utc>,
    priority: VisitPriority,
}

impl VisitEntry {
    pub fn new(when: &DateTime<Utc>, priority: VisitPriority) -> Self {
        Self {
            timestamp: *when,
            priority,
        }
    }
}

pub struct ObjectScore {
    visit_count: u32,
    entries: Vec<VisitEntry>,
}

fn weight_for(when: &DateTime<Utc>) -> u32 {
    let days = (Utc::now() - *when).num_days();
    if days <= 4 {
        100
    } else if days <= 14 {
        70
    } else if days <= 31 {
        50
    } else if days <= 90 {
        30
    } else {
        10
    }
}

impl Default for ObjectScore {
    fn default() -> Self {
        Self {
            visit_count: 0,
            entries: Vec::with_capacity(MAX_VISIT_ENTRIES),
        }
    }
}

impl ObjectScore {
    pub fn add(&mut self, entry: &VisitEntry) {
        // Remove the oldest entry to make room for the new one.
        if self.entries.len() == MAX_VISIT_ENTRIES {
            let _ = self.entries.remove(0);
        }

        self.entries.push(entry.clone());
        self.visit_count += 1;
    }

    pub fn frecency(&self) -> u32 {
        // For each sampled visit, the score is (bonus / 100.0) * weight
        // The final score for each item is ceiling(total visit count * sum of points for sampled visits / number of sampled visits)

        let sum = (&self.entries)
            .iter()
            .map(|item| (item.priority.bonus() * weight_for(&item.timestamp)) as f32 / 100.0)
            .sum::<f32>();

        self.visit_count * sum.round() as u32 / self.entries.len() as u32
    }

    #[cfg(test)]
    pub fn max() -> u32 {
        let mut score = ObjectScore::default();
        let now = Utc::now();
        for _i in 0..MAX_VISIT_ENTRIES {
            score.add(&VisitEntry::new(&now, VisitPriority::VeryHigh));
        }
        score.frecency()
    }
}

#[test]
fn frecency_alg() {
    use chrono::Duration;

    assert_eq!(ObjectScore::max(), 2000);

    // Add 2 visits of normal priority with a 10 day interval.
    let mut score = ObjectScore::default();

    let now = Utc::now();
    score.add(&VisitEntry::new(&now, VisitPriority::Normal));
    assert_eq!(score.frecency(), 100);

    score.add(&VisitEntry::new(
        &(now - Duration::days(10)),
        VisitPriority::Normal,
    ));
    assert_eq!(score.frecency(), 170);

    // Add 2 visits with a 10 day interval, one with high priority.
    let mut score = ObjectScore::default();

    let now = Utc::now();
    score.add(&VisitEntry::new(&now, VisitPriority::Normal));
    assert_eq!(score.frecency(), 100);

    score.add(&VisitEntry::new(
        &(now - Duration::days(10)),
        VisitPriority::High,
    ));
    assert_eq!(score.frecency(), 205);
}
