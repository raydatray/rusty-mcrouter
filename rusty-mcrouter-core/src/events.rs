use rusty_mcrouter_observability_primitives::EventSink;
use rusty_mcrouter_protocol::RequestKind;

use crate::FailoverPolicyKind;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutingEvent {
    FailoverTargetsExhausted,
}

#[derive(Clone, Copy, Debug)]
pub struct RoutingEventRecord {
    pub event: RoutingEvent,
    pub policy: FailoverPolicyKind,
    pub command: RequestKind,
}

pub type RoutingEventSink = EventSink<RoutingEventRecord>;
