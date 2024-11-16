# Open Air Quality Data Pipeline Project

This project demonstrates the creation of a beginner-friendly data pipeline using Open Air Quality data from a public S3 bucket. It aims to balance simplicity for newcomers with enough complexity to introduce important data engineering concepts. The pipeline extracts, transforms, and visualizes data in near real-time, ensuring the dashboard reflects live updates as data evolves.

- **OpenAQ S3 Archive Documentation**: [Learn more here](https://docs.openaq.org/aws/about)  
- **YouTube Tutorial**: [Watch the full tutorial here](link)

---

## Project Goals

1. **Educational Focus**: 
   - Introduce data engineering concepts without overwhelming beginners.
   - Provide a hands-on learning experience with practical tools and workflows.

2. **Tools and Technologies**: 
   - **Python**: For scripting (CLI apps), orchestration, and dashboard creation using Plotly Dash.
   - **DuckDB**: Lightweight, in-process database engine serving as the data warehouse.

3. **End Result**:
   - A functional data pipeline that extracts, transforms, and visualizes air quality data dynamically.

---

## Project Structure

- **`notebooks/`**: Scratchpads for experimenting with ideas and testing technologies.
- **`sql/`**: SQL scripts for data extraction and transformation, written in DuckDB’s query language.
- **`pipeline/`**: CLI applications for executing extraction, transformation, and database management tasks.
- **`dashboard/`**: Plotly Dash code for creating the live air quality dashboard.
- **`locations.json`**: Configuration file containing air quality sensor locations.
- **`secrets-example.json`**: Example configuration for OpenAQ API keys (**Note:** Do not commit actual secrets to version control).
- **`requirements.txt`**: List of Python libraries and dependencies.

---

## Database Structure

The DuckDB database includes the following schemas and tables:

1. **`raw` schema**:
   - Contains a single table with all extracted data.

2. **`presentation` schema**:
   - **`air_quality`**: The most recent version of each record per location.
   - **`daily_air_quality_stats`**: Daily averages for parameters at each location.
   - **`latest_param_values_per_location`**: Latest values for each parameter at each location.

---

## Running the Project

Follow these steps to set up and run the project:

1. **Set Up Python Environment**:
   - Create a virtual environment:
     ```bash
     $ python -m venv .venv
     ```
   - Activate the environment:
     - **Windows**: `$ . .venv/Scripts/activate`
     - **Linux/Mac**: `$ . .venv/bin/activate`
   - Install dependencies:
     ```bash
     $ pip install -r requirements.txt
     ```

2. **Initialize the Database**:
   - Navigate to the `pipeline` directory:
     ```bash
     $ cd pipeline
     ```
   - Run the database manager CLI to create the database:
     ```bash
     $ python database_manager.py --create
     ```

3. **Extract Data**:
   - Run the extraction CLI:
     ```bash
     $ python extraction.py [required arguments]
     ```

4. **Transform Data**:
   - Run the transformation CLI to create views in the presentation schema:
     ```bash
     $ python transformation.py
     ```

5. **Set Up the Dashboard**:
   - Navigate to the `dashboard` directory:
     ```bash
     $ cd dashboard
     ```
   - Start the dashboard application:
     ```bash
     $ python app.py
     ```

6. **Access the Results**:
   - The database will be stored as a `.db` file.
   - The dashboard will be accessible in your web browser.

---

## Additional Notes

- Ensure **Python 3.8+** is installed.
- Replace placeholders (e.g., API keys) in `secrets-example.json` with your actual credentials.
- Regularly update dependencies by running `pip install --upgrade -r requirements.txt`.

---

This project is designed to give you practical experience in building and managing a data pipeline. Happy learning!
