# Airflow DAG Authoring Snippets

📂 **This repository contains a collection of Airflow DAG examples created by me while studying for the DAG Authoring Certification.**

These examples showcase various use cases, best practices, and advanced features of Airflow.

## 📁 Repository Structure

Each file in this repository represents a different example or use case:

- **`ext_python_op.py`** 🐍
  - Demonstrates the use of the `ExternalPythonOperator` to execute Python scripts in a virtual environment.
  - Example includes dynamic task arguments and error handling for JSON configuration.

- **`my_dag.py`** 🛠️
  - Basic example of a DAG with a custom `PostgresOperator` and usage of Airflow variables for parameterization.

- **`my_xcom_dag.py`** 🔗
  - Showcases how to pass data between tasks using XComs for dynamic workflows.

- **`sensor_dag.py`** ⏱️
  - Demonstrates the use of sensors (e.g., `DateTimeSensor`) and dynamic task creation for multiple partners.

- **`sub_dag.py`** 🔄
  - Example of a SubDag implementation for modular workflows.

- **`suc_fail_dag.py`** ⚙️
  - Highlights the usage of callbacks for success, failure, and SLA misses, along with advanced retry configurations.

- **`task_f_dag.py`** 🧩
  - Illustrates task chaining using Python decorators and XCom-less task communication.

- **`task_g_dag.py`** 📊
  - Focuses on grouping and processing tasks dynamically for different partners.

- **`teste_dag.py`** 🧪
  - A testing DAG that demonstrates conditional task execution and triggering other DAGs dynamically.

## ⚙️ How to Use

1. Clone the repository:

   ```bash
   git clone https://github.com/AlefRP/Airflow_DAG_Snippets.git
   cd airflow-dag-snippets
   ```

2. Set up Airflow with Astro CLI:

   ```bash
   astro dev start
   ```

3. Access the Airflow UI using [http://localhost:8080](http://localhost:8080).

## ✨ Key Features

- Usage of Airflow variables for dynamic configuration.
- Integration of external Python scripts using `ExternalPythonOperator`.
- Modular design with SubDags for better organization.
- Error handling and retry strategies for robust workflows.
- Advanced task dependencies and sensors for real-time checks.

## 📜 License

This repository is licensed under the MIT License. See the [LICENSE.md](LICENSE) file for details.

---

Feel free to contribute to this repository by adding more examples or enhancing the existing ones. Happy DAG authoring!
