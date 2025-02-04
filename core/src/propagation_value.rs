use crate::connection_info::ContactInfo;
use bincode::serialize;
use morgan_interface::pubkey::Pubkey;
use morgan_interface::signature::{Keypair, Signable, Signature};
use morgan_interface::transaction::Transaction;
use std::{collections::BTreeSet, time::Duration};
use std::fmt;
use prometheus::{
    core::{Collector, Desc},
    proto::MetricFamily,
    Histogram, HistogramOpts, HistogramTimer, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts,
};

/// CrdsValue that is replicated across the cluster
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum CrdsValue {
    /// * Merge Strategy - Latest wallclock is picked
    ContactInfo(ContactInfo),
    /// * Merge Strategy - Latest wallclock is picked
    Vote(Vote),
    /// * Merge Strategy - Latest wallclock is picked
    EpochSlots(EpochSlots),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct EpochSlots {
    pub from: Pubkey,
    pub root: u64,
    pub slots: BTreeSet<u64>,
    pub signature: Signature,
    pub wallclock: u64,
}

impl EpochSlots {
    pub fn new(from: Pubkey, root: u64, slots: BTreeSet<u64>, wallclock: u64) -> Self {
        Self {
            from,
            root,
            slots,
            signature: Signature::default(),
            wallclock,
        }
    }
}

impl Signable for EpochSlots {
    fn pubkey(&self) -> Pubkey {
        self.from
    }

    fn signable_data(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct SignData<'a> {
            root: u64,
            slots: &'a BTreeSet<u64>,
            wallclock: u64,
        }
        let data = SignData {
            root: self.root,
            slots: &self.slots,
            wallclock: self.wallclock,
        };
        serialize(&data).expect("unable to serialize EpochSlots")
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature;
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Vote {
    pub from: Pubkey,
    pub transaction: Transaction,
    pub signature: Signature,
    pub wallclock: u64,
}

impl Vote {
    pub fn new(from: &Pubkey, transaction: Transaction, wallclock: u64) -> Self {
        Self {
            from: *from,
            transaction,
            signature: Signature::default(),
            wallclock,
        }
    }
}

impl Signable for Vote {
    fn pubkey(&self) -> Pubkey {
        self.from
    }

    fn signable_data(&self) -> Vec<u8> {
        #[derive(Serialize)]
        struct SignData<'a> {
            transaction: &'a Transaction,
            wallclock: u64,
        }
        let data = SignData {
            transaction: &self.transaction,
            wallclock: self.wallclock,
        };
        serialize(&data).expect("unable to serialize Vote")
    }

    fn get_signature(&self) -> Signature {
        self.signature
    }

    fn set_signature(&mut self, signature: Signature) {
        self.signature = signature
    }
}

pub struct DurationHistogram {
    histogram: Histogram,
}

impl DurationHistogram {
    pub fn new(histogram: Histogram) -> DurationHistogram {
        DurationHistogram { histogram }
    }

    pub fn observe_duration(&self, d: Duration) {
        // Duration is full seconds + nanos elapsed from the presious full second
        let v = d.as_secs() as f64 + f64::from(d.subsec_nanos()) / 1e9;
        self.histogram.observe(v);
    }
}

#[derive(Clone)]
pub struct OpMetrics {
    module: String,
    counters: IntCounterVec,
    gauges: IntGaugeVec,
    peer_gauges: IntGaugeVec,
    duration_histograms: HistogramVec,
}

impl OpMetrics {
    pub fn new<S: Into<String>>(name: S) -> OpMetrics {
        let name_str = name.into();
        OpMetrics {
            module: name_str.clone(),
            counters: IntCounterVec::new(
                Opts::new(name_str.clone(), format!("Counters for {}", name_str)),
                &["op"],
            )
            .unwrap(),
            gauges: IntGaugeVec::new(
                Opts::new(
                    format!("{}_gauge", name_str.clone()),
                    format!("Gauges for {}", name_str),
                ),
                &["op"],
            )
            .unwrap(),
            peer_gauges: IntGaugeVec::new(
                Opts::new(
                    format!("{}_peer_gauge", name_str.clone()),
                    format!("Gauges of each remote peer for {}", name_str),
                ),
                &["op", "remote_peer_id"],
            )
            .unwrap(),
            duration_histograms: HistogramVec::new(
                HistogramOpts::new(
                    format!("{}_duration", name_str.clone()),
                    format!("Histogram values for {}", name_str),
                ),
                &["op"],
            )
            .unwrap(),
        }
    }

    pub fn new_and_registered<S: Into<String>>(name: S) -> OpMetrics {
        let op_metrics = OpMetrics::new(name);
        prometheus::register(Box::new(op_metrics.clone()))
            .expect("OpMetrics registration on Prometheus failed.");
        op_metrics
    }

    #[inline]
    pub fn gauge(&self, name: &str) -> IntGauge {
        self.gauges.with_label_values(&[name])
    }

    #[inline]
    pub fn peer_gauge(&self, name: &str, remote_peer_id: &str) -> IntGauge {
        self.peer_gauges.with_label_values(&[name, remote_peer_id])
    }

    #[inline]
    pub fn counter(&self, name: &str) -> IntCounter {
        self.counters.with_label_values(&[name])
    }

    #[inline]
    pub fn histogram(&self, name: &str) -> Histogram {
        self.duration_histograms.with_label_values(&[name])
    }

    pub fn duration_histogram(&self, name: &str) -> DurationHistogram {
        DurationHistogram::new(self.duration_histograms.with_label_values(&[name]))
    }

    #[inline]
    pub fn inc(&self, op: &str) {
        self.counters.with_label_values(&[op]).inc();
    }

    #[inline]
    pub fn inc_by(&self, op: &str, v: usize) {
        // The underlying method is expecting i64, but most of the types
        // we're going to log are `u64` or `usize`.
        self.counters.with_label_values(&[op]).inc_by(v as i64);
    }

    #[inline]
    pub fn add(&self, op: &str) {
        self.gauges.with_label_values(&[op]).inc();
    }

    #[inline]
    pub fn sub(&self, op: &str) {
        self.gauges.with_label_values(&[op]).dec();
    }

    #[inline]
    pub fn set(&self, op: &str, v: usize) {
        // The underlying method is expecting i64, but most of the types
        // we're going to log are `u64` or `usize`.
        self.gauges.with_label_values(&[op]).set(v as i64);
    }

    #[inline]
    pub fn observe(&self, op: &str, v: f64) {
        self.duration_histograms.with_label_values(&[op]).observe(v);
    }

    pub fn observe_duration(&self, op: &str, d: Duration) {
        // Duration is full seconds + nanos elapsed from the presious full second
        let v = d.as_secs() as f64 + f64::from(d.subsec_nanos()) / 1e9;
        self.duration_histograms.with_label_values(&[op]).observe(v);
    }

    pub fn timer(&self, op: &str) -> HistogramTimer {
        self.duration_histograms
            .with_label_values(&[op])
            .start_timer()
    }
}

impl Collector for OpMetrics {
    fn desc(&self) -> Vec<&Desc> {
        let mut ms = Vec::with_capacity(4);
        ms.extend(self.counters.desc());
        ms.extend(self.gauges.desc());
        ms.extend(self.peer_gauges.desc());
        ms.extend(self.duration_histograms.desc());
        ms
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut ms = Vec::with_capacity(4);
        ms.extend(self.counters.collect());
        ms.extend(self.gauges.collect());
        ms.extend(self.peer_gauges.collect());
        ms.extend(self.duration_histograms.collect());
        ms
    }
}

/// Type of the replicated value
/// These are labels for values in a record that is associated with `Pubkey`
#[derive(PartialEq, Hash, Eq, Clone, Debug)]
pub enum CrdsValueLabel {
    ContactInfo(Pubkey),
    Vote(Pubkey),
    EpochSlots(Pubkey),
}

impl fmt::Display for CrdsValueLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrdsValueLabel::ContactInfo(_) => write!(f, "ContactInfo({})", self.pubkey()),
            CrdsValueLabel::Vote(_) => write!(f, "Vote({})", self.pubkey()),
            CrdsValueLabel::EpochSlots(_) => write!(f, "EpochSlots({})", self.pubkey()),
        }
    }
}

impl CrdsValueLabel {
    pub fn pubkey(&self) -> Pubkey {
        match self {
            CrdsValueLabel::ContactInfo(p) => *p,
            CrdsValueLabel::Vote(p) => *p,
            CrdsValueLabel::EpochSlots(p) => *p,
        }
    }
}

impl CrdsValue {
    /// Totally unsecure unverfiable wallclock of the node that generated this message
    /// Latest wallclock is always picked.
    /// This is used to time out push messages.
    pub fn wallclock(&self) -> u64 {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.wallclock,
            CrdsValue::Vote(vote) => vote.wallclock,
            CrdsValue::EpochSlots(vote) => vote.wallclock,
        }
    }
    pub fn label(&self) -> CrdsValueLabel {
        match self {
            CrdsValue::ContactInfo(contact_info) => {
                CrdsValueLabel::ContactInfo(contact_info.pubkey())
            }
            CrdsValue::Vote(vote) => CrdsValueLabel::Vote(vote.pubkey()),
            CrdsValue::EpochSlots(slots) => CrdsValueLabel::EpochSlots(slots.pubkey()),
        }
    }
    pub fn contact_info(&self) -> Option<&ContactInfo> {
        match self {
            CrdsValue::ContactInfo(contact_info) => Some(contact_info),
            _ => None,
        }
    }
    pub fn vote(&self) -> Option<&Vote> {
        match self {
            CrdsValue::Vote(vote) => Some(vote),
            _ => None,
        }
    }
    pub fn epoch_slots(&self) -> Option<&EpochSlots> {
        match self {
            CrdsValue::EpochSlots(slots) => Some(slots),
            _ => None,
        }
    }
    /// Return all the possible labels for a record identified by Pubkey.
    pub fn record_labels(key: &Pubkey) -> [CrdsValueLabel; 3] {
        [
            CrdsValueLabel::ContactInfo(*key),
            CrdsValueLabel::Vote(*key),
            CrdsValueLabel::EpochSlots(*key),
        ]
    }
}

impl Signable for CrdsValue {
    fn sign(&mut self, keypair: &Keypair) {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.sign(keypair),
            CrdsValue::Vote(vote) => vote.sign(keypair),
            CrdsValue::EpochSlots(epoch_slots) => epoch_slots.sign(keypair),
        };
    }

    fn verify(&self) -> bool {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.verify(),
            CrdsValue::Vote(vote) => vote.verify(),
            CrdsValue::EpochSlots(epoch_slots) => epoch_slots.verify(),
        }
    }

    fn pubkey(&self) -> Pubkey {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.pubkey(),
            CrdsValue::Vote(vote) => vote.pubkey(),
            CrdsValue::EpochSlots(epoch_slots) => epoch_slots.pubkey(),
        }
    }

    fn signable_data(&self) -> Vec<u8> {
        unimplemented!()
    }

    fn get_signature(&self) -> Signature {
        match self {
            CrdsValue::ContactInfo(contact_info) => contact_info.get_signature(),
            CrdsValue::Vote(vote) => vote.get_signature(),
            CrdsValue::EpochSlots(epoch_slots) => epoch_slots.get_signature(),
        }
    }

    fn set_signature(&mut self, _: Signature) {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::connection_info::ContactInfo;
    use crate::test_tx::test_tx;
    use bincode::deserialize;
    use morgan_interface::signature::{Keypair, KeypairUtil};
    use morgan_interface::timing::timestamp;

    #[test]
    fn test_labels() {
        let mut hits = [false; 3];
        // this method should cover all the possible labels
        for v in &CrdsValue::record_labels(&Pubkey::default()) {
            match v {
                CrdsValueLabel::ContactInfo(_) => hits[0] = true,
                CrdsValueLabel::Vote(_) => hits[1] = true,
                CrdsValueLabel::EpochSlots(_) => hits[2] = true,
            }
        }
        assert!(hits.iter().all(|x| *x));
    }
    #[test]
    fn test_keys_and_values() {
        let v = CrdsValue::ContactInfo(ContactInfo::default());
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().contact_info().unwrap().id;
        assert_eq!(v.label(), CrdsValueLabel::ContactInfo(key));

        let v = CrdsValue::Vote(Vote::new(&Pubkey::default(), test_tx(), 0));
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().vote().unwrap().from;
        assert_eq!(v.label(), CrdsValueLabel::Vote(key));

        let v = CrdsValue::EpochSlots(EpochSlots::new(Pubkey::default(), 0, BTreeSet::new(), 0));
        assert_eq!(v.wallclock(), 0);
        let key = v.clone().epoch_slots().unwrap().from;
        assert_eq!(v.label(), CrdsValueLabel::EpochSlots(key));
    }
    #[test]
    fn test_signature() {
        let keypair = Keypair::new();
        let wrong_keypair = Keypair::new();
        let mut v =
            CrdsValue::ContactInfo(ContactInfo::new_localhost(&keypair.pubkey(), timestamp()));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
        v = CrdsValue::Vote(Vote::new(&keypair.pubkey(), test_tx(), timestamp()));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
        let btreeset: BTreeSet<u64> = vec![1, 2, 3, 6, 8].into_iter().collect();
        v = CrdsValue::EpochSlots(EpochSlots::new(keypair.pubkey(), 0, btreeset, timestamp()));
        verify_signatures(&mut v, &keypair, &wrong_keypair);
    }

    fn test_serialize_deserialize_value(value: &mut CrdsValue, keypair: &Keypair) {
        let num_tries = 10;
        value.sign(keypair);
        let original_signature = value.get_signature();
        for _ in 0..num_tries {
            let serialized_value = serialize(value).unwrap();
            let deserialized_value: CrdsValue = deserialize(&serialized_value).unwrap();

            // Signatures shouldn't change
            let deserialized_signature = deserialized_value.get_signature();
            assert_eq!(original_signature, deserialized_signature);

            // After deserializing, check that the signature is still the same
            assert!(deserialized_value.verify());
        }
    }

    fn verify_signatures(
        value: &mut CrdsValue,
        correct_keypair: &Keypair,
        wrong_keypair: &Keypair,
    ) {
        assert!(!value.verify());
        value.sign(&correct_keypair);
        assert!(value.verify());
        value.sign(&wrong_keypair);
        assert!(!value.verify());
        test_serialize_deserialize_value(value, correct_keypair);
    }
}
