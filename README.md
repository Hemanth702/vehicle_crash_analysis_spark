# Vehicle Crash Analysis

## Overview
This project analyzes vehicle crash data to identify trends and patterns in crashes, injuries, and fatalities. It aims to provide insights into contributing factors and demographics involved in crashes.

## Project Structure
- `config/`: Contains configuration files for input and output paths.
- `data/`: Directory for input CSV files.
- `output/`: Directory for saving analysis results and visualizations.
- `src/`: Contains source code modules for data cleaning, analysis, and visualization.

## Setup

### Prerequisites
- **Apache Spark:** Ensure you have Apache Spark installed on your machine. You can follow the installation instructions on the [Apache Spark website](https://spark.apache.org/downloads.html).
- **Java:** Apache Spark requires Java to be installed. Download and install the latest version of [Java JDK](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html) or use a package manager if you're on Linux.
- **Python:** Make sure you have Python installed. You can download it from [python.org](https://www.python.org/downloads/).

### Installation Steps
1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
2. Install Python dependencies:
```bash
pip install -r requirements.txt
```
3.Running the Application:
```bash
spark-submit --master local main.py
```
4.Notes: 
The --master local option is used to run Spark locally. You can change this depending on your Spark cluster configuration.
Ensure that the configuration files in the config/ directory point to the correct input CSV files and output paths.