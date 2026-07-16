#![no_main]

use libfuzzer_sys::fuzz_target;
use pgshard_pgwire::{
    ClientEncoding, PgOutputConfiguration, PgOutputDecoder, PgOutputEncoding, PgOutputMessage,
    PgOutputStreaming, PgOutputTuple, PgOutputVersion,
};

fuzz_target!(|input: &[u8]| {
    let client_encoding =
        ClientEncoding::require_utf8("UTF8").expect("fixed canonical client encoding");
    let encoding = PgOutputEncoding::require_utf8(client_encoding, "UTF8")
        .expect("fixed canonical server encoding");

    for configuration in configurations() {
        exercise_input(input, configuration, encoding);
    }
});

fn configurations() -> [PgOutputConfiguration; 18] {
    [
        // Every decoder-relevant streaming/two-phase/message combination.
        configuration(
            PgOutputVersion::V1,
            PgOutputStreaming::Off,
            false,
            false,
            false,
        ),
        configuration(
            PgOutputVersion::V1,
            PgOutputStreaming::Off,
            false,
            false,
            true,
        ),
        configuration(
            PgOutputVersion::V1,
            PgOutputStreaming::Off,
            false,
            true,
            false,
        ),
        configuration(
            PgOutputVersion::V1,
            PgOutputStreaming::Off,
            false,
            true,
            true,
        ),
        configuration(
            PgOutputVersion::V2,
            PgOutputStreaming::On,
            false,
            false,
            false,
        ),
        configuration(
            PgOutputVersion::V2,
            PgOutputStreaming::On,
            false,
            true,
            false,
        ),
        configuration(
            PgOutputVersion::V2,
            PgOutputStreaming::On,
            false,
            false,
            true,
        ),
        configuration(
            PgOutputVersion::V2,
            PgOutputStreaming::On,
            false,
            true,
            true,
        ),
        configuration(
            PgOutputVersion::V4,
            PgOutputStreaming::Parallel,
            false,
            false,
            false,
        ),
        configuration(
            PgOutputVersion::V4,
            PgOutputStreaming::Parallel,
            false,
            false,
            true,
        ),
        configuration(
            PgOutputVersion::V4,
            PgOutputStreaming::Parallel,
            false,
            true,
            false,
        ),
        configuration(
            PgOutputVersion::V4,
            PgOutputStreaming::Parallel,
            false,
            true,
            true,
        ),
        // Additional valid versions and requested-vs-slot two-phase provenance.
        configuration(
            PgOutputVersion::V2,
            PgOutputStreaming::Off,
            false,
            false,
            false,
        ),
        configuration(
            PgOutputVersion::V3,
            PgOutputStreaming::Off,
            false,
            false,
            false,
        ),
        configuration(
            PgOutputVersion::V4,
            PgOutputStreaming::Off,
            false,
            false,
            false,
        ),
        configuration(
            PgOutputVersion::V3,
            PgOutputStreaming::Off,
            true,
            false,
            false,
        ),
        configuration(
            PgOutputVersion::V3,
            PgOutputStreaming::On,
            true,
            false,
            true,
        ),
        configuration(
            PgOutputVersion::V4,
            PgOutputStreaming::Parallel,
            true,
            false,
            false,
        ),
    ]
}

fn configuration(
    version: PgOutputVersion,
    streaming: PgOutputStreaming,
    requested_two_phase: bool,
    slot_two_phase: bool,
    messages: bool,
) -> PgOutputConfiguration {
    PgOutputConfiguration::new(
        version,
        streaming,
        requested_two_phase,
        slot_two_phase,
        messages,
    )
    .expect("fixed valid pgoutput fuzz configuration")
}

fn exercise_input(input: &[u8], configuration: PgOutputConfiguration, encoding: PgOutputEncoding) {
    let mut whole = PgOutputDecoder::new(configuration, encoding);
    if let Ok(message) = whole.decode(input) {
        exercise_message(message);
    }

    let mut sequence = PgOutputDecoder::new(configuration, encoding);
    let mut remaining = input;
    while let Some((&length, rest)) = remaining.split_first() {
        let length = usize::from(length).min(rest.len());
        let (message, tail) = rest.split_at(length);
        if let Ok(message) = sequence.decode(message) {
            exercise_message(message);
        }
        remaining = tail;
    }
    let _ = std::hint::black_box(sequence.finish());
}

fn exercise_message(message: PgOutputMessage<'_>) {
    match message {
        PgOutputMessage::Control(control) => {
            std::hint::black_box(control);
        }
        PgOutputMessage::Relation(relation) => {
            for column in relation.columns() {
                std::hint::black_box(column.expect("decoded Relation iterator invariant"));
            }
        }
        PgOutputMessage::Type(value) => {
            std::hint::black_box(value);
        }
        PgOutputMessage::Insert(insert) => exercise_tuple(insert.new_tuple()),
        PgOutputMessage::Update(update) => {
            if let Some(old) = update.old_tuple() {
                exercise_tuple(old.tuple());
            }
            exercise_tuple(update.new_tuple());
        }
        PgOutputMessage::Delete(delete) => exercise_tuple(delete.old_tuple().tuple()),
        PgOutputMessage::Truncate(truncate) => {
            for relation_id in truncate.relation_ids() {
                std::hint::black_box(relation_id.expect("decoded Truncate iterator invariant"));
            }
        }
        PgOutputMessage::LogicalMessage(message) => {
            std::hint::black_box(message);
        }
    }
}

fn exercise_tuple(tuple: PgOutputTuple<'_>) {
    for column in tuple.columns() {
        std::hint::black_box(column.expect("decoded tuple iterator invariant"));
    }
}
