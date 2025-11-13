<h1 align="center">📦 Event-driven Data Pipeline for E-commerce</h1>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.8%2B-blue" alt="Python">
  <img src="https://img.shields.io/badge/Google%20BigQuery-orange" alt="BigQuery">
  <img src="https://img.shields.io/badge/Streamlit-1.0%2B-brightgreen" alt="Streamlit">
</p>

<hr>

<h2>🧩 Overview</h2>

<p>This project implements a <strong>real-time event-driven data pipeline</strong> for e-commerce analytics using Python, BigQuery, and Streamlit. It ingests user interaction events and transaction data (Brazilian E-commerce Dataset), processes them via a <strong>Medallion Architecture</strong> (Bronze, Silver, Gold layers), and enables analytics on customer journeys, shopping funnels, product performance, and conversion rates.</p>

<h3>🎯 Key Goals</h3>
<ul>
  <li>Capture and process events in near real-time.</li>
  <li>Implement incremental loading with robust error handling.</li>
  <li>Build dimensional models for efficient analytics.</li>
  <li>Provide dashboards for actionable insights.</li>
</ul>

<p>The pipeline supports <strong>sessionization</strong> and <strong>SCD Type 2</strong> for historical tracking of dimensions.</p>

<hr>

<h2>⚙️ Features</h2>

<ul>
  <li><strong>Event Ingestion:</strong> Real-time capture of user events, simulated with Python (Kafka / Pub/Sub ready).</li>
  <li><strong>Data Processing Pipeline:</strong> Multi-layer ETL with Pandas, PySpark, and BigQuery SQL.</li>
  <li><strong>Medallion Architecture:</strong>
    <ul>
      <li><strong>Bronze:</strong> Raw ingestion</li>
      <li><strong>Silver:</strong> Cleansed, validated, and modeled fact/dim tables</li>
      <li><strong>Gold:</strong> Aggregated analytics / data marts</li>
    </ul>
  </li>
  <li><strong>Error Handling & Incremental Loads:</strong> Logging, retries, deduplication, and delta updates.</li>
  <li><strong>Analytics & Visualization:</strong> Streamlit dashboards for funnel analysis, KPIs, and insights.</li>
  <li><strong>Query Optimization:</strong> Partitioning and clustering in BigQuery.</li>
</ul>

<hr>

<h2>🛠️ Tech Stack</h2>

<table>
<thead>
<tr>
<th>Category</th>
<th>Tools / Technologies</th>
</tr>
</thead>
<tbody>
<tr><td>Ingestion</td><td>Python (Faker / requests), Brazilian E-commerce Dataset</td></tr>
<tr><td>Processing</td><td>Python, Pandas, PySpark, SQL (BigQuery)</td></tr>
<tr><td>Storage</td><td>Google BigQuery</td></tr>
<tr><td>Orchestration</td><td>Airflow</td></tr>
<tr><td>Visualization</td><td>Streamlit</td></tr>
<tr><td>Monitoring</td><td>BigQuery Logs, Python Logging</td></tr>
</tbody>
</table>

<hr>

<h2>🏗️ Architecture</h2>

<h3>🟤 Bronze Layer (Raw Ingestion)</h3>
<ul>
<li>Landing zone for raw event data.</li>
<li>Sources: Brazilian E-commerce Dataset (CSV/JSON), simulated events.</li>
<li>Implementation: Python scripts → BigQuery tables</li>
</ul>

<pre><code>CREATE TABLE bronze_events (
  event_id STRING,
  user_id STRING,
  event_type STRING,
  timestamp TIMESTAMP,
  raw_payload STRING
);
</code></pre>

<h3>⚪ Silver Layer (Cleansed & Modeled)</h3>
<ul>
<li>Cleaned and enriched fact/dim tables.</li>
<li>Transformations: Deduplication, enrichment, sessionization.</li>
<li>Dimensional modeling: Star schema with SCD Type 2.</li>
</ul>

<h3>🟡 Gold Layer (Aggregated Analytics)</h3>
<ul>
<li>Business-ready data marts for analytics.</li>
<li>Key tables: <code>gold_funnel_metrics</code>, <code>gold_product_performance</code>, <code>gold_conversion_rates</code></li>
</ul>

<h3>📈 Data Flow</h3>
<pre>
Raw Events/Transactions (Brazilian Dataset)
        ↓
Bronze Layer (BigQuery Raw Tables)
        ↓
Silver Layer (Fact/Dim Tables with SCD2)
        ↓
Gold Layer (Aggregated Data Marts)
        ↓
Streamlit Dashboard (Visualizations)
</pre>

<hr>

<h2>⚡ Setup & Installation</h2>

<h3>Prerequisites</h3>
<ul>
<li>Python 3.8+</li>
<li>Google Cloud SDK + BigQuery access</li>
<li>Python packages: streamlit, pandas, pyspark, google-cloud-bigquery</li>
</ul>

<h3>Clone & Install</h3>
<pre><code>git clone https://github.com/Vishnukothapalle/Event-driven-Data-Pipeline-for-E-commerce.git
cd Event-driven-Data-Pipeline-for-E-commerce
pip install -r requirements.txt
</code></pre>

<h3>Environment Variables (.env)</h3>
<pre><code>GOOGLE_APPLICATION_CREDENTIALS=path_TO_YOUR_KEY.json
PROJECT_ID=your-gcp-project
DATASET_ID=ecommerce_pipeline
</code></pre>

<h3>Run the Pipeline</h3>
<ul>
<li>Ingest to Bronze: <code>python scripts/ingest_bronze.py</code></li>
<li>Transform to Silver: <code>python scripts/etl_silver.py</code></li>
<li>Aggregate to Gold: <code>python scripts/aggregate_gold.py</code></li>
<li>Launch Dashboard: <code>streamlit run app/dashboard.py</code></li>
</ul>

<h3>Optional Orchestration</h3>
<ul>
<li>Use Airflow DAGs in <code>dags/</code> for scheduling pipelines.</li>
</ul>

<hr>

<h2>💻 Usage</h2>
<ul>
<li>Query analytics from BigQuery: <code>SELECT * FROM gold_funnel_metrics WHERE date = CURRENT_DATE()</code></li>
<li>View Streamlit Dashboard at <code>https://ecommercevisualizationdashboard-d2xkxijt98hozg6bbm6p83.streamlit.app/</code></li>
<li>Ingest Brazilian E-commerce dataset via ingestion scripts.</li>
</ul>



<hr>

<h2>📊 Outcomes & Learnings</h2>
<ul>
<li>Real-time processing of events and sessionization</li>
<li>Data marts for funnels, conversions, and product performance</li>
<li>Partitioning and clustering to optimize queries</li>
<li>SCD Type 2 handling for historical dimension changes</li>
<li>Scalable to 1M+ events/day</li>
</ul>

<hr>

<h2>🚀 Future Enhancements</h2>
<ul>
<li>Integrate Kafka / PubSub for real-time ingestion</li>
<li>Add ML-driven recommendation analytics</li>
<li>Deploy Streamlit dashboard online</li>
<li>Automated testing with Great Expectations</li>
</ul>

<hr>

<h2>🤝 Contributing</h2>
<p>Fork the repo, raise issues, or submit PRs. Follow PEP8 and include tests.</p>

<h2>📜 License</h2>
<p>MIT License – see <a href="LICENSE">LICENSE</a></p>

<h2>🙏 Acknowledgments</h2>
<ul>
<li>Brazilian E-commerce Dataset by Olist</li>
<li>Google Cloud BigQuery support</li>
</ul>

<p align="center">💡 Built with ❤️ by <strong>Vishnu Vardhan</strong> for Data Engineering Enthusiasts</p>
