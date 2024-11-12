# YCombinatroix

This project analyzes YCombinator companies to identify common characteristics and trends. It utilizes datasets of companies, founders, industries, and more, and processes data using Kafka for real-time streaming.

## Table of Contents

- [Project Description](#project-description)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Dataset Source](#dataset-source)

## Project Description

The YCombinator Companies Analysis project aims to provide insights into the characteristics of companies that have gone through the YCombinator accelerator. The project uses datasets related to companies, founders, industries, and more, and processes data using Kafka for real-time data streaming and analysis.

## Features

- Load and process datasets of YCombinator companies.
- Real-time data streaming using Apache Kafka.
- Filter companies by industry.
- Analyze common characteristics of successful startups.

## Installation

### Prerequisites

- Python 3.x
- Apache Kafka
- Zookeeper (for Kafka)
- Docker
- Java Development Kit (JDK) 8 or later
### Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/YCombinator-Analysis.git
   cd YCombinator-Analysis
   ```

2. **Create and activate a virtual environment:**

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install the required Python packages:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Install Java:**

   - **Windows**: Download and install the JDK from the [Oracle website](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html) or [OpenJDK](https://openjdk.java.net/). Set the `JAVA_HOME` environment variable to the JDK installation path.
   - **macOS/Linux**: Use a package manager like `brew` or `apt` to install OpenJDK. Set the `JAVA_HOME` environment variable in your shell profile.

5. **Start Zookeeper and Kafka:**

   ```bash
   # Start Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties

   # Start Kafka broker
   bin/kafka-server-start.sh config/server.properties
   ```

## Usage

1. **Load datasets and start the analysis:**

   ```bash
   python main.py
   ```

2. The script will load the datasets and filter companies by the specified industry.

## Configuration

- **Kafka Configuration**: Ensure that the `bootstrap_servers` parameter in your Kafka consumer setup matches your Kafka broker's address and port.
- **Datasets**: Place your datasets in the `datasets` directory.

## Dataset Source

- Ensure your datasets are correctly formatted and placed in the specified directory for processing.

