use std::collections::HashMap;
use polars_core::prelude::*;
use pyo3::{prelude::*, wrap_pyfunction};
use pyo3_asyncio;

use polars::prelude::*;
// use pyo3::prelude::*;
use pyo3_polars::{PyDataFrame, PySeries};
mod data_manipulation;
mod av;

#[pyfunction]
fn get_data(py: Python<'_>) -> PyResult<&PyAny> {
    // Create a simple Arrow Int64Array
    pyo3_asyncio::tokio::future_into_py(py, async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        // return Ok(());
        let data = snp500_data::fetcher::snp_data().await.unwrap();
        let sector_groups = snp500_data::group::by_sector(&data);

        // println!("{:?}", sector_groups.groups().unwrap());
        // println!("{:?}", sector_groups.keys());
        // println!("{:?}", sector_groups.get_groups());

        
        let group_filter = vec!["Energy"];
        let mut sector_data_map: HashMap<String, Vec<DataFrame>> = data_manipulation::groupByToHashMap(
            data.clone(), 
            sector_groups,
            group_filter
        ).await.unwrap();


        println!("Map Keys: {:?}", sector_data_map.keys());
        for key in sector_data_map.keys().into_iter() {
            println!("{} | {:?} Elements", key, sector_data_map.get(key).unwrap().len())
        }

        let perc_hashmap = data_manipulation::to_pctchg_hashmap(&mut sector_data_map);
        println!("{}", perc_hashmap.get("Energy").unwrap().len());
        
        
        let avg_hashmap = data_manipulation::avg_dfs(perc_hashmap);
        println!("Map Keys: {:?}", avg_hashmap.keys());
        for key in avg_hashmap.keys().into_iter() {
            // println!("{} | {:?} Elements", key, avg_hashmap.get(key).unwrap().head(Some(5)))
        }

        Ok(PyDataFrame(data))
                
                
        })
    
        
}

/// A Python module implemented in Rust.
#[pymodule]
fn rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_data, m)?)?;
    Ok(())
}
