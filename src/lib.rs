use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::{prelude::*};
use pyo3::types::{PyAny, PyBool, PyDict, PyFunction, PyModule};
use tokio::runtime::{Runtime};
use once_cell::sync::Lazy;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Duration;


static TOKIO_RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create tokio runtime.")
});

mod internal {
    use pyo3::prelude::*;
    use pyo3::types::{PyFunction, PyList, PyModule, PyString, PyAny};

    pub fn setup_python_path(py: Python<'_>) -> PyResult<()> {
        let sys: Bound<'_, PyModule>= PyModule::import(py, "sys")?;
        let path: Bound<'_, PyList> = sys.getattr("path")?.downcast_into::<PyList>()?;
        
        path.insert(0, PyString::new(py, "."))?;
        Ok(())
    }

    pub async fn exe_python_callable_async(
        py_func: Py<PyFunction>,
        arg: Py<PyAny>
    ) -> PyResult<()> {
        tokio::task::spawn_blocking(move || {
            Python::with_gil(|py_blocking| {
                py_func.call1(py_blocking, (arg,)).expect("Failed to call.");
            })
        })
        .await
        .map_err(|e: tokio::task::JoinError| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Blocking task panicked: {}", e)))?;

        Ok(())
    }
}

#[pyfunction]
fn new_thread(py: Python<'_>, py_func: Py<PyFunction>, arg: Py<PyAny>) -> PyResult<()> {
    internal::setup_python_path(py)?;

    let _: JoinHandle<Result<(), PyErr>> = TOKIO_RUNTIME.spawn(internal::exe_python_callable_async(py_func, arg));

    Ok(())
}

#[pyfunction]
fn fetch_metrics(py: Python<'_>) -> PyResult<Py<PyDict>> {
    let py_dict: Bound<'_, PyDict> = PyDict::new(py);
    let metrics = TOKIO_RUNTIME.metrics();
    
    py_dict.set_item("global_queue_depth", metrics.global_queue_depth())?;
    py_dict.set_item("num_alive_tasks", metrics.num_alive_tasks())?;
    py_dict.set_item("num_workers", metrics.num_workers())?;

    Ok(py_dict.unbind())
}

#[pyfunction]
fn wait_for_event(py: Python<'_>, arguments: Vec<i128>) -> PyResult<PyObject> {
    match arguments[0] {
        0x00 => {
            assert_eq!(arguments.len(), 2);

            let time_duration: Duration = Duration::from_nanos(arguments[1] as u64);
            
            let sleep_task = TOKIO_RUNTIME.spawn(async move {
                tokio::time::sleep(time_duration).await
            });

            let _ = TOKIO_RUNTIME.block_on(sleep_task)
                .map_err(|e: tokio::task::JoinError| {
                    PyErr::new::<PyRuntimeError, _>(
                        format!("Failed to wait for event (task join error): {}", e)
                    )
                })?;
        }
        other => {
            return Err(PyErr::new::<PyValueError, _>(
                format!("Unknown event id, '{other}'.")
            ));
        }
    }

    Ok(py.None())
}

#[pyclass(name = "MutexLock")]
#[derive(Clone)]
pub struct PyMutexLock {
    locked: Arc<AtomicBool>,
    notify: Arc<Notify>
}

#[pymethods]
impl PyMutexLock {
    #[new]
    fn new() -> Self {
        Self {
            locked: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn acquire(&self, py: Python<'_>) -> PyResult<()> {
        let s = self.clone();

        py.allow_threads(move || {
            TOKIO_RUNTIME.block_on(async move {
                loop {
                    if s.locked.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                        break
                    }

                    s.notify.notified().await;
                }
            });
        });

        Ok(())
    }

    pub fn release(&self, _py: Python<'_>) -> PyResult<()> {
        self.locked.store(false, Ordering::SeqCst);

        self.notify.notify_one();
        Ok(())
    }
    
    pub fn get_locked(&self, py: Python<'_>) -> Py<PyBool> {
        <pyo3::Bound<'_, PyBool> as Clone>::clone(&PyBool::new(py, self.locked.load(Ordering::SeqCst))).unbind()
    }
}

#[pymodule]
fn coil_core(_py: Python, m: Bound<PyModule>) -> PyResult<()> {
    pyo3::prepare_freethreaded_python();
    
    m.add_function(wrap_pyfunction!(new_thread, &m)?)?;
    m.add_function(wrap_pyfunction!(fetch_metrics, &m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_event, &m)?)?;

    m.add_class::<PyMutexLock>()?;

    Ok(())
}