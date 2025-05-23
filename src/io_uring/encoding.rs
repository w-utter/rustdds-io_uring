#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UserData {
    domain_id: u16,
    variant: user_data::Variant,
}

impl UserData {
    pub fn new(domain_id: u16, variant: user_data::Variant) -> Self {
        Self {
            domain_id,
            variant,
        }
    }
}

impl From<UserData> for u64 {
    fn from(udata: UserData) -> u64 {
        let UserData {
            domain_id,
            variant,
        } = udata;

        let domain_id = domain_id as u64;

        use user_data::*;
        let (variant, rest) = match variant {
            Variant::DataRecv(recv_kind) => {
                (1, match recv_kind {
                    UdpDataRecv::MulticastDiscovery => 1,
                    UdpDataRecv::UnicastDiscovery => 2,
                    UdpDataRecv::MulticastUserTraffic => 3,
                    UdpDataRecv::UnicastUserTraffic => 4,
                })
            }
            Variant::Timer(timer_kind) => {
                let (timer_variant, rest) = match timer_kind {
                    Timer::Read(eid, kind) => {
                        // since theres only 1 variant (for now?),
                        // we dont need to encode it into the user data
                        let read_timer_variant = match kind {
                            ReadTimerVariant::RequestedDeadline => 0,
                        };

                        let rest = eid.as_u32() as u64;
                        (1, rest)
                    }
                    Timer::Write(eid, kind) => {
                        let write_timer_variant = match kind {
                            WriteTimerVariant::Heartbeat => 0,
                            WriteTimerVariant::CacheCleaning => 1,
                            WriteTimerVariant::SendRepairData => 2,
                            WriteTimerVariant::SendRepairFrags => 3,
                        };
                        let rest = write_timer_variant | ((eid.as_u32() as u64) << 2);
                        (2, rest)
                    }
                    Timer::Builtin(kind) => {
                        let rest = match kind {
                            BuiltinTimerVariant::CacheCleaning => 1,
                            BuiltinTimerVariant::AckNack => 2,
                        };
                        (3, rest)
                    }
                };

                let rest = timer_variant | (rest << 3);
                (2, rest)
            }
        };
        let rest = (variant | rest << 3);
        domain_id | (rest << 16)
    }
}

impl TryFrom<u64> for UserData {
    type Error = ();

    fn try_from(raw: u64) -> Result<UserData, Self::Error> {
        let domain_id = (raw & 0xFFFF) as u16;

        let raw = raw >> 16;
        let raw_variant = raw & 0b111;
        let raw = raw >> 3;

        use user_data::*;
        let variant = match raw_variant {
            1 => {
                // recv
                let recv_kind = match raw & 0b111 {
                    1 => UdpDataRecv::MulticastDiscovery,
                    2 => UdpDataRecv::UnicastDiscovery,
                    3 => UdpDataRecv::MulticastUserTraffic,
                    4 => UdpDataRecv::UnicastUserTraffic,
                    _ => return Err(())
                };
                Variant::DataRecv(recv_kind)
            }
            2 => {
                // timer
                let raw_timer_variant = raw & 0b111;
                let raw = raw >> 3;

                use crate::structure::guid::EntityId;
                let timer_kind = match raw_timer_variant {
                    1 => {
                        // read
                        let eid = EntityId::from_u32(raw as u32);
                        let kind = ReadTimerVariant::RequestedDeadline;
                        Timer::Read(eid, kind)
                    }
                    2 => {
                        let write_variant = raw & 0b11;
                        let raw = raw >> 2;
                        let kind = match write_variant {
                            0 => WriteTimerVariant::Heartbeat,
                            1 => WriteTimerVariant::CacheCleaning,
                            2 => WriteTimerVariant::SendRepairData,
                            3 => WriteTimerVariant::SendRepairFrags,
                            _ => return Err(()),
                        };
                        let eid = EntityId::from_u32(raw as u32);
                        Timer::Write(eid, kind)
                    }
                    3 => {
                        // builtin
                        let kind = match raw & 0b111 {
                            1 => BuiltinTimerVariant::CacheCleaning,
                            2 => BuiltinTimerVariant::AckNack,
                            _ => return Err(()),
                        };
                        Timer::Builtin(kind)
                    }
                    _ => return Err(())
                };
                Variant::Timer(timer_kind)
            }
            _ => return Err(()),
        };


        Ok(UserData {
            variant,
            domain_id,
        })
    }
}

pub mod user_data {
    use crate::structure::guid::EntityId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Variant {
        DataRecv(UdpDataRecv),
        Timer(Timer),
    }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum UdpDataRecv {
        MulticastDiscovery,
        UnicastDiscovery,
        MulticastUserTraffic,
        UnicastUserTraffic,
    }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Timer {
        Read(EntityId, ReadTimerVariant),
        Write(EntityId, WriteTimerVariant),
        Builtin(BuiltinTimerVariant),
    }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ReadTimerVariant {
        RequestedDeadline,
    }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum WriteTimerVariant {
        Heartbeat,
        CacheCleaning,
        SendRepairData,
        SendRepairFrags,
    }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum BuiltinTimerVariant {
        AckNack,
        CacheCleaning,
    }
}

#[cfg(test)]
mod tests {
    // TODO: test this, then impl in the register fns.
    use super::*;
    use user_data::*;
    use crate::structure::guid::EntityId;

    macro_rules! mirror_udata {
        ($original:expr) => {
            let udata = u64::from($original);
            let mirror = UserData::try_from(udata).unwrap();
            assert_eq!($original, mirror);
        }
    }

    macro_rules! with_domain_ids {
        ($variant:expr) => {
            let domain_id = 0;
            let variant = UserData::new(domain_id, $variant);
            mirror_udata!(variant);

            let domain_id = u16::MAX;
            let variant = UserData::new(domain_id, $variant);
            mirror_udata!(variant);
        }
    }

    // making sure that decoding & encoding gives the same original output.

    #[test]
    fn data_recv() {
        with_domain_ids!(Variant::DataRecv(UdpDataRecv::MulticastDiscovery));

        with_domain_ids!(Variant::DataRecv(UdpDataRecv::UnicastDiscovery));

        with_domain_ids!(Variant::DataRecv(UdpDataRecv::MulticastUserTraffic));

        with_domain_ids!(Variant::DataRecv(UdpDataRecv::UnicastUserTraffic));
    }


    #[test]
    fn builtin_timer() {
        with_domain_ids!(Variant::Timer(Timer::Builtin(BuiltinTimerVariant::AckNack)));
        with_domain_ids!(Variant::Timer(Timer::Builtin(BuiltinTimerVariant::CacheCleaning)));
    }

    #[test]
    fn read_timer() {
        with_domain_ids!(Variant::Timer(Timer::Read(EntityId::MAX, ReadTimerVariant::RequestedDeadline)));

        with_domain_ids!(Variant::Timer(Timer::Read(EntityId::MIN, ReadTimerVariant::RequestedDeadline)));
    }

    #[test]
    fn write_timer() {
        with_domain_ids!(Variant::Timer(Timer::Write(EntityId::MAX, WriteTimerVariant::Heartbeat)));

        with_domain_ids!(Variant::Timer(Timer::Write(EntityId::MIN, WriteTimerVariant::Heartbeat)));

        with_domain_ids!(Variant::Timer(Timer::Write(EntityId::MAX, WriteTimerVariant::CacheCleaning)));

        with_domain_ids!(Variant::Timer(Timer::Write(EntityId::MIN, WriteTimerVariant::CacheCleaning)));

        with_domain_ids!(Variant::Timer(Timer::Write(EntityId::MAX, WriteTimerVariant::SendRepairData)));

        with_domain_ids!(Variant::Timer(Timer::Write(EntityId::MIN, WriteTimerVariant::SendRepairData)));

        with_domain_ids!(Variant::Timer(Timer::Write(EntityId::MAX, WriteTimerVariant::SendRepairFrags)));

        with_domain_ids!(Variant::Timer(Timer::Write(EntityId::MIN, WriteTimerVariant::SendRepairFrags)));
    }
}
