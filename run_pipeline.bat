@echo off
:: Move to project directory
cd /d "e:\DataEngineer\BigData\Class7"

:: Activate Anaconda environment
:: Check if this path exists on your machine
call C:\Users\HP\Anaconda3\Scripts\activate.bat spark_env

:: Set Python path for Spark
for /f "tokens=*" %%i in ('where python') do (
    set PYSPARK_PYTHON=%%i
    set PYSPARK_DRIVER_PYTHON=%%i
    goto :run
)

:run
:: Run the main pipeline
python main_pipeline.py

:: pause
