use std::path::PathBuf;

use anyhow::Result;
use chrono::{Duration, Utc};
use clap::{arg, Command};
use tokio::runtime::Runtime;

use crate::config::Settings;
use storb_validator::apikey::{ApiKeyConfig, ApiKeyManager};

pub fn cli() -> Command {
    Command::new("apikey")
        .about("Manage API keys for validator access")
        .arg(
            arg!(--"api-keys-db" <PATH> "Path to the API keys database")
                .required(false)
                .global(true)
                .id("api-keys-db"), // Add this line to set the argument ID
        )
        .subcommand(
            Command::new("create")
                .about("Create a new API key")
                .arg(arg!(-n --name <NAME> "Name for the API key"))
                .arg(
                    arg!(-e --expires <DAYS> "Expiration time in days")
                        .required(false)
                        .value_parser(clap::value_parser!(u64)),
                )
                .arg(
                    arg!(-r --"rate-limit" <RATE> "Rate limit in requests per minute")
                        .required(false)
                        .value_parser(clap::value_parser!(u32)),
                )
                .arg(
                    arg!(-u --"upload-limit" <BYTES> "Maximum upload bytes")
                        .required(false)
                        .value_parser(clap::value_parser!(u64)),
                )
                .arg(
                    arg!(-d --"download-limit" <BYTES> "Maximum download bytes")
                        .required(false)
                        .value_parser(clap::value_parser!(u64)),
                ),
        )
        .subcommand(Command::new("list").about("List all API keys"))
        .subcommand(
            Command::new("delete")
                .about("Delete an API key")
                .arg(arg!(<KEY> "API key to delete")),
        )
}

pub fn handle_command(matches: &clap::ArgMatches) -> Result<()> {
    // Create a new tokio runtime
    let rt = Runtime::new()?;

    // create new API key manager with the database path from args
    let db_path = matches
        .get_one::<String>("api-keys-db") // Use "api-keys-db" instead of "api_keys_db"
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let settings = Settings::new(None).expect("Failed to load settings");
            PathBuf::from(&settings.validator.api_keys_db)
        });
    let api_key_manager = ApiKeyManager::new(db_path).expect("Failed to create API key manager");

    match matches.subcommand() {
        Some(("create", create_matches)) => {
            let name = create_matches.get_one::<String>("name").unwrap();
            let expires_days = create_matches.get_one::<u64>("expires");
            let rate_limit = create_matches.get_one::<u32>("rate-limit").copied();
            let upload_limit = create_matches.get_one::<u64>("upload-limit").copied();
            let download_limit = create_matches.get_one::<u64>("download-limit").copied();

            let expires_at = expires_days.map(|days| Utc::now() + Duration::days(*days as i64));

            let key = rt.block_on(api_key_manager.create_key(ApiKeyConfig {
                name: name.clone(),
                expires_at,
                rate_limit,
                upload_limit,
                download_limit,
            }))?;

            println!("✨ Created API key: {}", key.key);
            println!("Name: {}", key.name);
            if let Some(expires) = key.expires_at {
                println!("Expires: {}", expires.format("%Y-%m-%d %H:%M:%S UTC"));
            }
            if let Some(rate) = key.rate_limit {
                println!("Rate limit: {} requests/minute", rate);
            }
            if let Some(limit) = key.upload_limit {
                println!("Upload limit: {} bytes", limit);
            }
            if let Some(limit) = key.download_limit {
                println!("Download limit: {} bytes", limit);
            }
        }
        Some(("list", _)) => {
            let keys = rt.block_on(api_key_manager.list_keys())?;
            if keys.is_empty() {
                println!("No API keys found");
                return Ok(());
            }

            println!("Found {} API keys:", keys.len());
            for key in keys {
                println!("\n🔑 Key: {}", key.key);
                println!("   Name: {}", key.name);
                println!(
                    "   Created: {}",
                    key.created_at.format("%Y-%m-%d %H:%M:%S UTC")
                );
                if let Some(expires) = key.expires_at {
                    println!("   Expires: {}", expires.format("%Y-%m-%d %H:%M:%S UTC"));
                }
                if let Some(rate) = key.rate_limit {
                    println!("   Rate limit: {} requests/minute", rate);
                }
                println!("   Upload used: {} bytes", key.upload_used);
                if let Some(limit) = key.upload_limit {
                    println!("   Upload limit: {} bytes", limit);
                }
                println!("   Download used: {} bytes", key.download_used);
                if let Some(limit) = key.download_limit {
                    println!("   Download limit: {} bytes", limit);
                }
            }
        }
        Some(("delete", delete_matches)) => {
            let key = delete_matches.get_one::<String>("KEY").unwrap();
            if rt.block_on(api_key_manager.delete_key(key))? {
                println!("✅ API key deleted successfully");
            } else {
                println!("❌ API key not found");
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}
