#Hate Crimes Analysis 📊🔍



Overview
Welcome to Hate Crimes Analysis, a real-time data streaming and transformation pipeline focused on hate crime data from 2017 to 2025. This project uses modern big data tools like Confluent Kafka, PySpark, and AWS S3 to process and analyze hate crimes data efficiently. Our goal is to build a scalable data pipeline that streams data, performs real-time transformations, and stores it in Parquet format, enabling further analysis and visualization.

🔍 Data Source
📅 Dataset: Hate Crimes 2017-2025 on Kaggle
This dataset includes records of hate crimes across the years, covering categories, descriptions, and other relevant details.

🚀 Key Features
✅ Data Streaming: Real-time ingestion using Confluent Kafka producers.
✅ PySpark Transformation: Data cleaning, enrichment, and transformation using PySpark.
✅ AWS S3 Integration: Storing transformed data in Parquet format in AWS S3 for efficient querying and downstream analysis.
✅ Visualization: Preparing visual insights (planned) from Parquet data in S3.
✅ Flexible Development: Using Google Colab for notebook-based development and VS Code for Kafka producers.

⚙️ Technology Stack
Confluent Kafka (Data Streaming)

PySpark (ETL and Transformations)

AWS S3 (Data Lake Storage in Parquet Format)

Google Colab (Notebook-based exploration and development)

VS Code (Producer scripting and Kafka integration)

Python 3.8+

📦 Installation
Clone the repository:

git clone https://github.com/ashrafali01/HateCrimesAnalysis.git
cd HateCrimesAnalysis
(Optional) Create a virtual environment:


python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
Install dependencies:


pip install -r requirements.txt
Set up Confluent Kafka (free tier or local) and configure producer topics.

Configure AWS credentials for S3 integration.

🖥️ Usage
Kafka Producer

Run kafka_producer.py from VS Code to send data from the dataset to Kafka.

PySpark Transformation

Execute spark_transform.py to read from Kafka, process data, and write to AWS S3 in Parquet format.

Visualization

Develop visualizations using notebooks or BI tools on the Parquet data stored in S3 (planned).

📊 Visualization
We plan to use the Parquet data in AWS S3 for visualizations, either using notebooks (Google Colab/Jupyter) or Tableau. The focus will be on trends, categories, and demographic factors in hate crimes over the years.

🤝 Contributions
Contributions are welcome! Please fork the repository, open issues, or submit pull requests to enhance features, fix bugs, or add documentation. See CONTRIBUTING.md for details.

📜 License
This project is licensed under the MIT License.

🙌 Acknowledgements
Data Source: Kaggle Dataset by sonawanelalitsunil

Libraries: Confluent Kafka, PySpark, AWS SDK, Pandas, etc.

Special thanks to the open-source community and all contributors.

📬 Contact
👤 Ashraf Ali
📧 ashrafali5530@gmail.com

