#  Automated ETL Pipeline & Local Data Lake (Medallion Architecture)

End-to-end automation for the extraction, transformation, and modeling of historical purchasing data from an ERP (SAP) to an interactive dashboard in Power BI.

**Technologies:** `Python` | `Power Automate` | `Power BI` | `DAX` | `Parquet`

---

##  Context & Business Problem
Analyzing historical purchases (January 2025 - April 2026) required significant manual effort. The reports exported from the ERP in Excel format contained inconsistencies, typos, and non-standardized dates. 

**Objective:** Eliminate manual intervention through an RPA process, clean the data, and model it analytically to address two key management requests:
1. Annual financial summary by supplier.
2. ABC inventory analysis for resource optimization.

---

## System Architecture (Medallion)

The project implements a local Medallion architecture (Bronze, Silver, Gold), achieving high efficiency and data compression without cloud infrastructure costs.

```mermaid
flowchart TD
    subgraph Data_Pipeline [Main Data Flow]
        direction LR
        SAP[ERP SAP] --> Source[Source File \n .xlsx]
        Source --> Bronze[(Bronze Layer \n Raw Data)]
        Bronze --> Silver[(Silver Layer \n Parquet)]
        Silver --> Gold[(Gold Layer \n Dimensional Model)]
        Gold --> BI[Reporting \n Power BI]
    end

    subgraph Orchestration [Automation & Processing]
        direction LR
        Power_Automate(Power Automate)
        Python(Python)
    end

    Power_Automate -. Extracts .-> SAP
    Power_Automate -. Moves .-> Source
    Power_Automate -. Orchestrates .-> Python

    Python -. Processes .-> Bronze
    Python -. Transforms .-> Silver
    Python -. Models .-> Gold

    classDef bronze fill:#CD7F32,stroke:#333,stroke-width:2px,color:#fff;
    classDef silver fill:#C0C0C0,stroke:#333,stroke-width:2px,color:#000;
    classDef gold fill:#FFD700,stroke:#333,stroke-width:2px,color:#000;
    classDef excel fill:#21A366,stroke:#333,stroke-width:2px,color:#fff;
    classDef bi fill:#F2C811,stroke:#333,stroke-width:2px,color:#000;

    class Bronze bronze;
    class Silver silver;
    class Gold gold;
    class Source excel;
    class BI bi;
