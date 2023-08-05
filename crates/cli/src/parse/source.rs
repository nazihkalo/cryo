use std::env;

use ethers::prelude::*;
use governor::{Quota, RateLimiter};
use polars::prelude::*;
use std::num::NonZeroU32;
use cryo_freeze::{ParseError, Source};


use crate::args::Args;


pub(crate) async fn parse_source(args: &Args) -> Result<Source, ParseError> {
    
    let rpc_url = parse_rpc_url(args);
    let provider = Provider::new_client(rpc_url.as_str(), 10, 500)
        .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))?;
    let chain_id = provider
        .get_chainid()
        .await
        .map_err(|_e| ParseError::ParseError("could not connect to provider".to_string()))?
        .as_u64();

    let rate_limiter = match args.requests_per_second {
        Some(rate_limit) => match NonZeroU32::new(rate_limit) {
            Some(value) => {
                let quota = Quota::per_second(value);
                Some(Arc::new(RateLimiter::direct(quota)))
            }
            _ => None,
        },
        None => None,
    };

    // process concurrency info
    let max_concurrent_requests = args.max_concurrent_requests.unwrap_or(100);
    let max_concurrent_chunks = args.max_concurrent_chunks.unwrap_or(3);

    let semaphore = tokio::sync::Semaphore::new(max_concurrent_requests as usize);
    let semaphore = Some(Arc::new(semaphore));

    let output = Source {
        provider: Arc::new(provider),
        chain_id,
        semaphore,
        rate_limiter,
        inner_request_size: args.inner_request_size,
        max_concurrent_chunks,
    };

    Ok(output)
}

fn parse_rpc_url(args: &Args) -> String {
    let mut url = match &args.rpc {
        Some(url) => url.clone(),
        _ => match env::var("ETH_RPC_URL") {
            Ok(url) => url,
            Err(_e) => {
                println!("must provide --rpc or set ETH_RPC_URL");
                std::process::exit(0);
            }
        },
    };
    if !url.starts_with("http") {
        url = "http://".to_string() + url.as_str();
    };
    url
}

#[cfg(test)]
mod tests {
    use super::*;
    // use ethers::prelude::*;
    use tokio::runtime::Runtime;
    const EXPECTED_CHAIN_ID: u64 = 11155111;

    fn create_args() -> Args {
        // fill all the fields as required
        Args {
            datatype: vec!["logs".to_string()],
            blocks: vec!["0:latest".to_string()],
            align: false,
            reorg_buffer: 0,
            include_columns: Some(vec!["from".to_string(), "to".to_string()]),
            exclude_columns: Some(vec!["gas".to_string()]),
            columns: None,
            hex: false,
            sort: None,
            rpc: Some("https://rpc2.sepolia.org".to_string()),
            network_name: Some("mainnet".to_string()),
            requests_per_second: Some(10),
            max_concurrent_requests: Some(10),
            max_concurrent_chunks: Some(3),
            dry: false,
            chunk_size: 1000,
            n_chunks: None,
            output_dir: ".".to_string(),
            file_suffix: None,
            overwrite: false,
            csv: false,
            json: false,
            row_group_size: None,
            n_row_groups: None,
            no_stats: false,
            compression: vec!["lz4".to_string()],
            contract: None,
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            inner_request_size: 1,
            no_verbose: false,
        }
    }

    #[test]
    fn test_parse_source_with_all_args_set() {
        let rt = Runtime::new().unwrap();
        let args = create_args();

        let source = rt.block_on(parse_source(&args));
        // assert that the source is Ok if not then print the error and panic
        assert!(source.is_ok(), "Error: {:?}", source.err()); 

        let source = source.unwrap();
        // assert individual fields of the source based on the args
        assert_eq!(source.chain_id, EXPECTED_CHAIN_ID);
        assert_eq!(source.semaphore.unwrap().available_permits(), 10); // expecting 10 concurrent requests
        assert_eq!(source.max_concurrent_chunks, 3);
        assert_eq!(source.inner_request_size, 1);
           
    }

    #[test]
    fn test_parse_source_with_some_default_args_unset() {
        let rt = Runtime::new().unwrap();
        let mut args = create_args();
        args.requests_per_second = None; //unset
        args.max_concurrent_requests = None; //unset
        args.max_concurrent_chunks = None; //unset

        let source = rt.block_on(parse_source(&args));
        assert!(source.is_ok(), "Error: {:?}", source.err()); 

        let source = source.unwrap();
        // assert individual fields of the source based on the args and default values
        assert_eq!(source.semaphore.unwrap().available_permits(), 100); // expecting 100 concurrent requests
        assert_eq!(source.max_concurrent_chunks, 3);
        //assert that the rate_limiter is None
        assert!(source.rate_limiter.is_none());

    }

    #[test]
    fn test_parse_source_with_invalid_rpc() {
        let rt = Runtime::new().unwrap();
        let mut args = create_args();
        args.rpc = Some("invalid_rpc".to_string()); //invalid

        let source = rt.block_on(parse_source(&args));
        assert!(source.is_err());

        match source {
            Err(ParseError::ParseError(msg)) => {
                assert_eq!(msg, "could not connect to provider");
            }
            _ => panic!("unexpected error type"),
        }
    }

    #[test]
    fn test_parse_source_with_env_var() {
        let rt = Runtime::new().unwrap();
        let mut args = create_args();
        args.rpc = None; //unset
        env::set_var("ETH_RPC_URL", "https://rpc2.sepolia.org");

        let source = rt.block_on(parse_source(&args));
        assert!(source.is_ok(), "Error: {:?}", source.err()); 

        env::remove_var("ETH_RPC_URL"); //clean up
    }

    #[test]
    #[should_panic(expected = "must provide --rpc or set ETH_RPC_URL")]
    fn test_parse_source_without_rpc_and_env_var() {
        let rt = Runtime::new().unwrap();
        let mut args = create_args();
        args.rpc = None; //unset
        env::remove_var("ETH_RPC_URL"); //unset

        let _ = rt.block_on(parse_source(&args)); //should panic
        

    }
}
