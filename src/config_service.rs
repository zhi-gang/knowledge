//! Manage configurations of Knowledge Application
//!
//! 
//! 

use std::fs::read_to_string;
use clap::ArgMatches;
use serde::Deserialize;
use tracing::{info, instrument};

/// Root struct of configurations
#[derive(Debug, Deserialize)]
pub struct KnowledgeConfig {
    pub http_service: ServiceConf,
}
/// Implementation of KnowledgeConfig 
impl KnowledgeConfig {
    #[instrument]
    pub fn load(path: &str) -> anyhow::Result<KnowledgeConfig> {
        info!(path, "Loading Configuration file");
        let content = read_to_string(path)?; //std::io::Error
        let decoded_config = toml::from_str(&content[..])?; //raise: toml::de::Error
        Ok(decoded_config)
    }
}

/// Struct containing confiturations of http service
#[derive(Debug, Deserialize)]
pub struct ServiceConf {
    pub host: String,
    pub port: u16,
}


impl ServiceConf {
    /// Build HTTP Service URL
    ///
    /// The arguments set in the command lines are overwritten the values in the configuration file.
    /// 
    /// # Arguments
    /// 
    /// * `matches`: Command line arguments
    /// 
    /// # Returns
    /// 
    /// URL of the HTTP service
    pub fn url(&self, matches: &ArgMatches) -> String {
        //if set in command line, use command settings otherwise use that in configuration file.
        let host = matches.get_one::<String>("service_host");
        let port = matches.get_one::<u16>("service_port");
        let mut url = if let Some(h) = host {
            String::from(h)
        } else {
            String::from(&self.host)
        };
        url.push(':');
        if let Some(p) = port {
            url.push_str(&p.to_string());
        } else {
            url.push_str(&self.port.to_string())
        };
        url
    }
}


#[cfg(test)]
mod config_test {
    use super::*;

    #[test]
    fn load_conf_test() {
        println!("Running test @ {:?}", std::env::current_dir().unwrap());
        let path = "configuration /config.toml";
        let cfg_result = KnowledgeConfig::load(path);
        let err = match cfg_result {
            Ok(conf) => {
                println!("{:?}", conf);
                assert_eq!(conf.http_service.port, 3000);
                // assert_eq!(conf.cache.size, 100);
                None
            }
            Err(err) => Some(err),
        };

        assert!(
            err.is_none(),
            "Failed to load configuration file. {:?}",
            err
        );
    }
}

// #[derive(Debug, Deserialize)]
// pub struct CacheConfiguration {
//     size: usize
// }