use std::collections::HashMap;
use polars_core::prelude::*;
use pyo3::{prelude::*, wrap_pyfunction};
use pyo3_asyncio;
use pyo3::types::PyList;

use pyo3_polars::{
    PyDataFrame,
    PySeries
};

mod data_manipulation;
mod av;

/// Formats the sum of two numbers as string.
// #[pyfunction]
// fn get_data(py: Python<'_>) -> PyResult<&PyAny>{
//     pyo3_asyncio::tokio::future_into_py(py, async {
//         tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        
//         let data = snp500_data::fetcher::snp_data().await.unwrap();

        
//         // let py_list = PyList::new(py, &[1, 2, 3]);
//         Ok(Python::with_gil(|py| py.None()))
//     })
    
//     // let sector_groups = snp500_data::group::by_sector(&data);

//     // // println!("{:?}", sector_groups.groups().unwrap());
//     // // println!("{:?}", sector_groups.keys());
//     // // println!("{:?}", sector_groups.get_groups());

    
//     // let group_filter = vec!["Energy"];
//     // let mut sector_data_map: HashMap<String, Vec<DataFrame>> = data_manipulation::groupByToHashMap(
//     //     data.clone(), 
//     //     sector_groups,
//     //     group_filter
//     // ).await.unwrap();


//     // println!("Map Keys: {:?}", sector_data_map.keys());
//     // for key in sector_data_map.keys().into_iter() {
//     //     println!("{} | {:?} Elements", key, sector_data_map.get(key).unwrap().len())
//     // }

//     // let perc_hashmap = data_manipulation::to_pctchg_hashmap(&mut sector_data_map);
//     // println!("{}", perc_hashmap.get("Energy").unwrap().len());
    
    
//     // let avg_hashmap = data_manipulation::avg_dfs(perc_hashmap);
//     // println!("Map Keys: {:?}", avg_hashmap.keys());
//     // for key in avg_hashmap.keys().into_iter() {
//     //     println!("{} | {:?} Elements", key, avg_hashmap.get(key).unwrap().head(Some(5)))
//     // }
// }

use libc::uintptr_t;
use arrow2::array::Int64Array;
use arrow_flight::FlightData;
// use arrow2_flight::flight::FlightData;
#[pyfunction]
fn get_arrow_data(py: Python<'_>) -> PyResult<PyObject> {
    // Create a simple Arrow Int64Array
    let data = vec![10, 20, 30, 40, 50];
    let arrow_array = Int64Array::from_slice(&data);
    let array = Arc::new(arrow_array);
    // let (array_pointer, schema_pointer) = array.to_raw().map_err(PyO3ArrowError::from)?;
    let pa = py.import("pyarrow")?;
    let array = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_pointer as uintptr_t, schema_pointer as uintptr_t),
    )?;
    Ok(array.to_object(py))

}

/// A Python module implemented in Rust.
#[pymodule]
fn string_sum(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(get_arrow_data, m)?)?;
    Ok(())
}