// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Read};
use std::path::Path;

use grammers_tl_gen::{Config, Outputs, generate_rust_code};
use grammers_tl_parser::parse_tl_file;
use grammers_tl_parser::tl::Definition;

/// Load the type language definitions from a certain file.
/// Parse errors will be printed to `stderr`, and only the
/// valid results will be returned.
fn load_tl(file: &str) -> io::Result<Vec<Definition>> {
    let mut file = File::open(file)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    Ok(parse_tl_file(&contents)
        .filter_map(|d| match d {
            Ok(d) => Some(d),
            Err(e) => {
                eprintln!("TL: parse error: {e:?}");
                None
            }
        })
        .collect())
}

/// Find the `// LAYER #` comment, and return its value if it's valid.
fn find_layer(file: &str) -> io::Result<Option<i32>> {
    const LAYER_MARK: &str = "LAYER";

    Ok(BufReader::new(File::open(file)?).lines().find_map(|line| {
        let line = line.unwrap();
        if line.trim().starts_with("//") {
            if let Some(pos) = line.find(LAYER_MARK) {
                if let Ok(layer) = line[pos + LAYER_MARK.len()..].trim().parse() {
                    return Some(layer);
                }
            }
        }

        None
    }))
}

fn main() -> std::io::Result<()> {
    let layer = match find_layer("tl/api.tl")? {
        Some(x) => x,
        None => panic!("no layer information found in api.tl"),
    };

    let definitions = {
        let mut definitions = Vec::new();
        if cfg!(feature = "tl-api") {
            definitions.extend(load_tl("tl/api.tl")?);
        }
        if cfg!(feature = "tl-mtproto") {
            definitions.extend(load_tl("tl/mtproto.tl")?);
        }
        definitions
    };

    let output_dir = Path::new(&env::var("OUT_DIR").unwrap()).to_path_buf();
    let mut outputs = Outputs {
        common: BufWriter::new(File::create(output_dir.join("generated_common.rs"))?),
        types: BufWriter::new(File::create(output_dir.join("generated_types.rs"))?),
        functions: BufWriter::new(File::create(output_dir.join("generated_functions.rs"))?),
        enums: BufWriter::new(File::create(output_dir.join("generated_enums.rs"))?),
    };

    let config = Config {
        gen_name_for_id: true,
        deserializable_functions: cfg!(feature = "deserializable-functions"),
        impl_debug: cfg!(feature = "impl-debug"),
        impl_from_enum: cfg!(feature = "impl-from-enum"),
        impl_from_type: cfg!(feature = "impl-from-type"),
        impl_serde: cfg!(feature = "impl-serde"),
    };

    generate_rust_code(&mut outputs, &definitions, layer, &config)?;

    outputs.flush()?;
    drop(outputs);

    // Support both new (Layer 228+) and legacy constructor IDs during deserialization.
    let enums_path = output_dir.join("generated_enums.rs");
    let enums_code = std::fs::read_to_string(&enums_path)?;
    let enums_code = enums_code
        .replace(
            "crate::types::Channel::CONSTRUCTOR_ID => Self::Channel(crate::types::Channel::deserialize(buf)?),",
            "crate::types::Channel::CONSTRUCTOR_ID | 0xd49f34c6 => Self::Channel(crate::types::Channel::deserialize(buf)?),",
        )
        .replace(
            "crate::types::User::CONSTRUCTOR_ID => Self::User(crate::types::User::deserialize(buf)?),",
            "crate::types::User::CONSTRUCTOR_ID | 0xb1b8cc83 => Self::User(crate::types::User::deserialize(buf)?),",
        );
    std::fs::write(&enums_path, enums_code)?;

    let common_path = output_dir.join("generated_common.rs");
    let common_code = std::fs::read_to_string(&common_path)?;
    let common_code = common_code
        .replace(
            "0x1c32b11c => \"channel\",",
            "0x1c32b11c | 0xd49f34c6 => \"channel\",",
        )
        .replace(
            "0x31774388 => \"user\",",
            "0x31774388 | 0xb1b8cc83 => \"user\",",
        );
    std::fs::write(&common_path, common_code)?;

    let types_path = output_dir.join("generated_types.rs");
    let mut types_code = std::fs::read_to_string(&types_path)?;
    // Channel: flags2.20 is linked_community_id:long in Layer 228+ (0xd49f34c6)
    let channel_needle = "        Ok(Channel {";
    let channel_pos = types_code
        .find("impl crate::Deserializable for Channel {")
        .expect("Channel Deserializable impl not found");
    let channel_ok_pos = types_code[channel_pos..]
        .find(channel_needle)
        .expect("Ok(Channel { not found")
        + channel_pos;
    types_code.insert_str(
        channel_ok_pos,
        "        if (flags2 & (1 << 20)) != 0 {\n            let _ = i64::deserialize(buf)?;\n        }\n",
    );

    // User: flags2.21 is linked_community_id:long in Layer 228+ (0xb1b8cc83)
    let user_needle = "        Ok(User {";
    let user_pos = types_code
        .find("impl crate::Deserializable for User {")
        .expect("User Deserializable impl not found");
    let user_ok_pos = types_code[user_pos..]
        .find(user_needle)
        .expect("Ok(User { not found")
        + user_pos;
    types_code.insert_str(
        user_ok_pos,
        "        if (flags2 & (1 << 21)) != 0 {\n            let _ = i64::deserialize(buf)?;\n        }\n",
    );
    std::fs::write(&types_path, types_code)?;

    Ok(())
}
