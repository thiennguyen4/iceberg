# Iceberg Demo - Apache Iceberg với INSERT, UPDATE, DELETE, BATCH INGESTION, EXPIRE SNAPSHOTS, SNAPSHOT HISTORY, TIME TRAVEL

## Overview

Project này demo đầy đủ các tính năng của **Apache Iceberg** bao gồm:
- ✅ INSERT - Thêm dữ liệu mới
- ✅ BATCH INGESTION - Thêm nhiều dữ liệu cùng lúc
- ✅ UPDATE - Cập nhật dữ liệu
- ✅ DELETE - Xóa dữ liệu
- ✅ MERGE - Upsert dữ liệu (insert hoặc update)
- ✅ QUERY - Lọc dữ liệu với điều kiện phức tạp
- ✅ SNAPSHOT HISTORY - Xem lịch sử các snapshot
- ✅ TIME TRAVEL - Truy vấn dữ liệu tại một thời điểm trong quá khứ
- ✅ EXPIRE SNAPSHOTS - Xóa snapshots cũ (by time, by count, hybrid)

Tech stack: **Java 17**, **Spring Boot 4.0.2**, **Apache Spark 3.5.0**, **Apache Iceberg 1.4.3**, **MinIO**

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          CLIENT (HTTP)                               │
│                      POST /orders (JSON)                             │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SPRING BOOT (API Layer)                           │
│  - OrderController: Handles HTTP requests                            │
│  - Deserializes JSON to CreateOrderRequest                           │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   SERVICE LAYER (Business Logic)                     │
│  - OrderService: Orchestrates Iceberg operations                     │
│  - Creates namespace and table if needed                             │
│  - Executes INSERT via Spark SQL                                     │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│              APACHE SPARK (Compute Engine)                           │
│  - Embedded SparkSession (local mode)                                │
│  - Executes SQL: CREATE NAMESPACE, CREATE TABLE, INSERT              │
│  - Generates Parquet data files                                      │
│  - Calls Iceberg API to commit transaction                           │
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│            APACHE ICEBERG (Table Format)                             │
│  - Manages table schema and evolution                                │
│  - Creates new SNAPSHOT (immutable version)                          │
│  - Generates manifest files (lists data files)                       │
│  - Generates manifest list (lists manifest files)                    │
│  - Creates new metadata.json file                                    │
│  - Provides ACID guarantees                                          │
└───────────────────────┬─────────────────────┬───────────────────────┘
                        │                     │
                        ▼                     ▼
┌────────────────────────────────┐  ┌────────────────────────────────┐
│  REST CATALOG (Metadata)       │  │  MinIO (S3 Storage)            │
│  - Stores metadata POINTERS    │  │  - Stores ACTUAL FILES:        │
│  - Table locations             │  │    1. Data files (*.parquet)   │
│  - Current metadata file path  │  │    2. Manifest files (*.avro)  │
│  - Namespace management        │  │    3. Manifest lists (*.avro)  │
│  - NO actual data stored       │  │    4. Metadata JSON files      │
└────────────────────────────────┘  └────────────────────────────────┘
```

## Tech Stack

- **Java 25** - Latest Java LTS version
- **Spring Boot 4.0.2** - REST API framework
- **Apache Spark 3.5.0** - Distributed compute engine
- **Apache Iceberg 1.4.3** - Table format with ACID guarantees
- **Iceberg REST Catalog** - Centralized metadata management
- **MinIO** - S3-compatible object storage
- **Maven** - Build tool
- **Lombok** - Reduces boilerplate code

## Project Structure

```
iceburg/
├── src/main/java/dev/onemount/iceburg/
│   ├── IceburgApplication.java          # Spring Boot main class
│   ├── config/
│   │   └── SparkConfig.java             # SparkSession configuration
│   ├── controller/
│   │   └── OrderController.java         # REST API endpoints
│   ├── dto/
│   │   ├── CreateOrderRequest.java      # API request DTO
│   │   └── CreateOrderResponse.java     # API response DTO
│   └── service/
│       └── OrderService.java            # Business logic with Iceberg
├── src/main/resources/
│   └── application.properties           # Configuration
├── pom.xml                              # Maven dependencies
└── docker-compose.yml                   # MinIO, REST Catalog, Spark containers
```

## Prerequisites

1. **Java 25** installed
2. **Maven 3.6+** installed
3. **Docker** and **Docker Compose** installed

## Setup & Run

### 1. Start Infrastructure (MinIO + REST Catalog)

```bash
# Start MinIO and Iceberg REST Catalog
docker-compose up -d

# Verify services are running
docker ps

# Check MinIO UI: http://localhost:9001 (admin/password)
# REST Catalog API: http://localhost:8181
```

### 2. Build the Application

```bash
mvn clean install
```

### 3. Run Spring Boot Application

```bash
mvn spring-boot:run
```

The API will start on **http://localhost:8090**

## API Endpoints

### 1. INSERT - Tạo Order Mới

**Endpoint:** `POST /orders`

```bash
curl -X POST http://localhost:8090/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORD-001",
    "customerId": "CUST-123",
    "productName": "Laptop",
    "quantity": 2,
    "price": 1299.99
  }'
```

**Response:**
```json
{
  "orderId": "ORD-001",
  "snapshotId": 8120351505078854920,
  "message": "Order created successfully and stored in Iceberg table"
}
```

### 2. BATCH INGESTION - Thêm Nhiều Orders Cùng Lúc

**Endpoint:** `POST /orders/batch`

```bash
curl -X POST http://localhost:8090/orders/batch \
  -H "Content-Type: application/json" \
  -d '{
    "orders": [
      {
        "orderId": "ORD-100",
        "customerId": "CUST-001",
        "productName": "Laptop",
        "quantity": 2,
        "price": 1299.99
      },
      {
        "orderId": "ORD-101",
        "customerId": "CUST-002",
        "productName": "Mouse",
        "quantity": 10,
        "price": 29.99
      },
      {
        "orderId": "ORD-102",
        "customerId": "CUST-003",
        "productName": "Keyboard",
        "quantity": 5,
        "price": 79.99
      }
    ]
  }'
```

**Response:**
```json
{
  "message": "Batch ingest completed",
  "recordsProcessed": 3,
  "snapshotId": 8120351505078854921
}
```

**Lợi ích:**
- Xử lý hàng nghìn/hàng triệu records trong một transaction
- Tối ưu hiệu suất với Spark DataFrame API
- Chỉ tạo 1 snapshot cho toàn bộ batch (thay vì N snapshots)
- Phù hợp cho ETL và data pipeline

### 3. GET - Lấy Tất Cả Orders

**Endpoint:** `GET /orders`

```bash
curl http://localhost:8090/orders
```

**Response:**
```json
[
  {
    "orderId": "ORD-001",
    "customerId": "CUST-123",
    "productName": "Laptop",
    "quantity": 2,
    "price": 1299.99,
    "orderTimestamp": "2026-01-27 10:00:00.0"
  }
]
```

### 4. UPDATE - Cập Nhật Order

**Endpoint:** `PUT /orders/{orderId}`

```bash
curl -X PUT http://localhost:8090/orders/ORD-001 \
  -H "Content-Type: application/json" \
  -d '{
    "quantity": 5,
    "price": 1199.99
  }'
```

**Response:**
```json
{
  "orderId": "ORD-001",
  "snapshotId": 8120351505078854921,
  "message": "Order updated successfully"
}
```

### 5. DELETE - Xóa Order

**Endpoint:** `DELETE /orders/{orderId}`

```bash
curl -X DELETE http://localhost:8090/orders/ORD-001
```

**Response:**
```json
{
  "orderId": "ORD-001",
  "snapshotId": 8120351505078854922,
  "message": "Order deleted successfully"
}
```

### 6. SNAPSHOT HISTORY - Xem Lịch Sử Snapshots

**Endpoint:** `GET /orders/snapshots`

```bash
curl http://localhost:8090/orders/snapshots
```

**Response:**
```json
[
  {
    "snapshotId": 8120351505078854922,
    "committedAt": "2026-01-27 10:05:00.0",
    "operation": "delete"
  },
  {
    "snapshotId": 8120351505078854921,
    "committedAt": "2026-01-27 10:03:00.0",
    "operation": "overwrite"
  },
  {
    "snapshotId": 8120351505078854920,
    "committedAt": "2026-01-27 10:00:00.0",
    "operation": "append"
  }
]
```

### 7. TIME TRAVEL - Xem Dữ Liệu Tại Snapshot Cụ Thể

**Endpoint:** `GET /orders/time-travel/snapshot/{snapshotId}`

```bash
curl http://localhost:8090/orders/time-travel/snapshot/8120351505078854920
```

**Response:**
```json
[
  {
    "orderId": "ORD-001",
    "customerId": "CUST-123",
    "productName": "Laptop",
    "quantity": 2,
    "price": 1299.99,
    "orderTimestamp": "2026-01-27 10:00:00.0"
  }
]
```

### 8. TIME TRAVEL - Xem Dữ Liệu Tại Timestamp Cụ Thể

**Endpoint:** `GET /orders/time-travel/timestamp/{timestamp}`

```bash
curl http://localhost:8090/orders/time-travel/timestamp/2026-01-27%2010:02:00
```

**Response:** Trả về dữ liệu như nó tồn tại tại thời điểm timestamp đó

### 9. EXPIRE SNAPSHOTS BY TIME - Xóa Snapshots Theo Thời Gian

**Endpoint:** `POST /orders/snapshots/expire/by-time`

```bash
curl -X POST http://localhost:8090/orders/snapshots/expire/by-time \
  -H "Content-Type: application/json" \
  -d '{
    "olderThanTimestamp": "2026-01-20 00:00:00"
  }'
```

**Response:**
```json
{
  "message": "Snapshots expired by time successfully",
  "snapshotsExpired": 5,
  "snapshotsRetained": 3
}
```

**Lợi ích:**
- Xóa tất cả snapshots cũ hơn thời gian UTC chỉ định
- Format: 'YYYY-MM-DD HH:MM:SS' (ví dụ: '2026-01-20 00:00:00')
- Tiết kiệm storage bằng cách cleanup dữ liệu không còn cần thiết
- Phù hợp cho retention policy dựa trên thời gian cố định

### 10. EXPIRE SNAPSHOTS BY COUNT - Xóa Snapshots Giữ Lại Số Lượng

**Endpoint:** `POST /orders/snapshots/expire/by-count`

```bash
curl -X POST http://localhost:8090/orders/snapshots/expire/by-count \
  -H "Content-Type: application/json" \
  -d '{
    "retainLast": 5
  }'
```

**Response:**
```json
{
  "message": "Snapshots expired by count successfully",
  "snapshotsExpired": 10,
  "snapshotsRetained": 5
}
```

**Lợi ích:**
- Giữ lại N snapshots mới nhất
- Đảm bảo luôn có số lượng snapshots tối thiểu cho rollback
- Dễ dàng quản lý số lượng versions

### 11. EXPIRE SNAPSHOTS HYBRID - Xóa Theo Thời Gian Nhưng Giữ Số Lượng

**Endpoint:** `POST /orders/snapshots/expire/hybrid`

```bash
curl -X POST http://localhost:8090/orders/snapshots/expire/hybrid \
  -H "Content-Type: application/json" \
  -d '{
    "olderThanTimestamp": "2026-01-22 00:00:00",
    "retainLast": 3
  }'
```

**Response:**
```json
{
  "message": "Snapshots expired with hybrid strategy successfully",
  "snapshotsExpired": 7,
  "snapshotsRetained": 3
}
```

**Lợi ích:**
- Kết hợp cả hai strategies: time-based và count-based
- Xóa snapshots cũ hơn thời gian chỉ định (UTC) NHƯNG luôn giữ lại N snapshots mới nhất
- Linh hoạt nhất: cleanup dữ liệu cũ nhưng đảm bảo có đủ snapshots để recovery
- Best practice cho production: ví dụ xóa snapshot trước 2026-01-22 nhưng giữ lại tối thiểu 3 snapshots
- Format timestamp: 'YYYY-MM-DD HH:MM:SS' (UTC timezone)

### 12. Health Check

**Endpoint:** `GET /orders/health`

```bash
curl http://localhost:8090/orders/health
```

## Demo Workflow - Test Tất Cả Tính Năng

### Bước 1: Tạo Orders (INSERT)

```bash
# Tạo order 1
curl -X POST http://localhost:8090/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-001", "customerId": "CUST-123", "productName": "Laptop", "quantity": 2, "price": 1299.99}'

# Tạo order 2
curl -X POST http://localhost:8090/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-002", "customerId": "CUST-456", "productName": "Mouse", "quantity": 5, "price": 29.99}'

# Tạo order 3
curl -X POST http://localhost:8090/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-003", "customerId": "CUST-789", "productName": "Keyboard", "quantity": 3, "price": 79.99}'
```

### Bước 2: Batch Ingestion - Thêm Nhiều Orders Cùng Lúc

```bash
curl -X POST http://localhost:8090/orders/batch \
  -H "Content-Type: application/json" \
  -d '{
    "orders": [
      {"orderId": "ORD-100", "customerId": "CUST-100", "productName": "Monitor", "quantity": 2, "price": 299.99},
      {"orderId": "ORD-101", "customerId": "CUST-101", "productName": "Webcam", "quantity": 4, "price": 89.99},
      {"orderId": "ORD-102", "customerId": "CUST-102", "productName": "Headset", "quantity": 3, "price": 149.99},
      {"orderId": "ORD-103", "customerId": "CUST-103", "productName": "Docking Station", "quantity": 1, "price": 199.99},
      {"orderId": "ORD-104", "customerId": "CUST-104", "productName": "USB Hub", "quantity": 10, "price": 39.99}
    ]
  }'
```

### Bước 3: Xem Tất Cả Orders

```bash
curl http://localhost:8090/orders
```

### Bước 4: Cập Nhật Order (UPDATE)

```bash
curl -X PUT http://localhost:8090/orders/ORD-001 \
  -H "Content-Type: application/json" \
  -d '{"quantity": 5, "price": 1199.99}'
```

### Bước 5: Xóa Order (DELETE)

```bash
curl -X DELETE http://localhost:8090/orders/ORD-002
```

### Bước 6: Xem Snapshot History

```bash
curl http://localhost:8090/orders/snapshots
```

### Bước 7: Time Travel - Xem Dữ Liệu Trước Khi Update

```bash
# Lấy snapshot ID đầu tiên từ history
curl http://localhost:8090/orders/time-travel/snapshot/{snapshotId}
```

### Bước 8: Expire Snapshots - Xóa Snapshots Cũ

```bash
# Xóa snapshots cũ hơn ngày 2026-01-22 nhưng giữ lại 3 snapshots mới nhất
curl -X POST http://localhost:8090/orders/snapshots/expire/hybrid \
  -H "Content-Type: application/json" \
  -d '{
    "olderThanTimestamp": "2026-01-22 00:00:00",
    "retainLast": 3
  }'

# Hoặc xóa theo thời gian (UTC)
curl -X POST http://localhost:8090/orders/snapshots/expire/by-time \
  -H "Content-Type: application/json" \
  -d '{"olderThanTimestamp": "2026-01-25 00:00:00"}'

# Hoặc giữ lại N snapshots mới nhất
curl -X POST http://localhost:8090/orders/snapshots/expire/by-count \
  -H "Content-Type: application/json" \
  -d '{"retainLast": 5}'
```

## Giải Thích Các Tính Năng

### INSERT
- Mỗi INSERT tạo một snapshot mới
- Dữ liệu được ghi vào file Parquet trong MinIO
- Iceberg tạo manifest và metadata files
- ACID: Transaction đảm bảo all-or-nothing

### BATCH INGESTION
- Sử dụng Spark DataFrame API để xử lý hàng loạt
- Tất cả records trong batch được commit trong 1 transaction
- Chỉ tạo 1 snapshot cho toàn bộ batch (hiệu quả hơn nhiều lần INSERT đơn lẻ)
- Tối ưu cho ETL pipeline, data migration, bulk load
- Hỗ trợ xử lý hàng nghìn đến hàng triệu records

### UPDATE
- Iceberg thực hiện copy-on-write hoặc merge-on-read
- Tạo snapshot mới với dữ liệu đã update
- Dữ liệu cũ vẫn còn trong snapshot trước (cho time travel)

### DELETE
- Tạo snapshot mới đánh dấu rows đã xóa
- Dữ liệu vật lý vẫn còn (cho time travel)
- Có thể expire snapshots cũ để xóa thật sự

### SNAPSHOT HISTORY
- Mỗi operation (INSERT/UPDATE/DELETE) tạo snapshot mới
- Mỗi snapshot có ID unique và timestamp
- Có thể track operation type (append/overwrite/delete)
- Dùng để audit trail và compliance

### TIME TRAVEL
- Query dữ liệu tại bất kỳ snapshot nào
- 2 cách: theo snapshot ID hoặc timestamp
- Useful cho:
  - Debug: "Dữ liệu trước khi bug là gì?"
  - Audit: "Ai thay đổi gì lúc nào?"
  - Recovery: "Rollback về version trước"
  - Analysis: So sánh data giữa các thời điểm

### EXPIRE SNAPSHOTS
- **By Time**: Xóa snapshots cũ hơn timestamp UTC chỉ định (format: 'YYYY-MM-DD HH:MM:SS')
- **By Count**: Giữ lại N snapshots mới nhất, xóa tất cả snapshots cũ hơn
- **Hybrid**: Xóa snapshots theo timestamp UTC NHƯNG luôn giữ lại N snapshots mới nhất
- Lợi ích:
  - Tiết kiệm storage bằng cách cleanup snapshots không cần thiết
  - Tự động quản lý retention policy
  - Giảm chi phí lưu trữ cho production
  - Hybrid strategy là best practice: balance giữa cleanup và safety
- Use cases:
  - Production: Xóa snapshot trước 2026-01-01 nhưng giữ tối thiểu 10 snapshots
  - Dev/Test: Chỉ giữ 5 snapshots mới nhất
  - Compliance: Giữ snapshot theo quy định (90 ngày, 1 năm, etc.)
- Timestamp format: UTC 'YYYY-MM-DD HH:MM:SS' (ví dụ: '2026-01-27 03:02:00')

## Project Structure Mới

```
iceburg/
├── src/main/java/dev/onemount/iceburg/
│   ├── IceburgApplication.java
│   ├── config/
│   │   ├── AwsBootstrap.java
│   │   └── SparkConfig.java
│   ├── controller/
│   │   └── OrderController.java       # Thêm 6 endpoints mới
│   ├── dto/
│   │   ├── CreateOrderRequest.java
│   │   ├── CreateOrderResponse.java
│   │   ├── UpdateOrderRequest.java    # [MỚI] DTO cho UPDATE
│   │   ├── OrderResponse.java         # [MỚI] DTO cho GET responses
│   │   └── SnapshotInfo.java          # [MỚI] DTO cho snapshot history
│   └── service/
│       └── OrderService.java          # Thêm 6 methods mới
└── src/main/resources/
    └── application.properties
```

## Các Tính Năng Đã Thêm

✅ **OrderService.java**
- `getAllOrdersAsList()` - Lấy tất cả orders
- `updateOrder()` - Cập nhật order theo ID
- `deleteOrder()` - Xóa order theo ID
- `getSnapshotHistory()` - Lấy lịch sử snapshots
- `getOrdersAtSnapshot()` - Time travel theo snapshot ID
- `getOrdersAtTimestamp()` - Time travel theo timestamp

✅ **OrderController.java**
- `GET /orders` - Lấy tất cả orders
- `PUT /orders/{orderId}` - Update order
- `DELETE /orders/{orderId}` - Xóa order
- `GET /orders/snapshots` - Xem snapshot history
- `GET /orders/time-travel/snapshot/{snapshotId}` - Time travel by snapshot
- `GET /orders/time-travel/timestamp/{timestamp}` - Time travel by timestamp

✅ **DTOs Mới**
- `UpdateOrderRequest.java` - Request body cho UPDATE
- `OrderResponse.java` - Response cho GET operations
- `SnapshotInfo.java` - Snapshot metadata

## Data Flow Explanation

### Step-by-Step Flow

1. **HTTP Request Arrives**
   - Client sends POST request with JSON body to Spring Boot
   - Spring Boot deserializes JSON to `CreateOrderRequest` object

2. **Controller Layer**
   - `OrderController` receives the request
   - Validates and forwards to `OrderService`

3. **Service Layer - Namespace Creation**
   ```sql
   CREATE NAMESPACE IF NOT EXISTS rest.sales
   ```
   - REST Catalog registers namespace
   - No files created yet (just metadata in catalog)

4. **Service Layer - Table Creation**
   ```sql
   CREATE TABLE IF NOT EXISTS rest.sales.orders (
     order_id STRING,
     customer_id STRING,
     product_name STRING,
     quantity INT,
     price DECIMAL(10,2),
     order_timestamp TIMESTAMP
   ) USING iceberg
   ```
   - REST Catalog registers table
   - Iceberg creates initial metadata file in MinIO:
     - `s3://warehouse/sales/orders/metadata/v1.metadata.json`
   - Metadata contains: schema, partition spec, no snapshots yet

5. **Service Layer - Data Insert**
   ```sql
   INSERT INTO rest.sales.orders 
   VALUES ('ORD-001', 'CUST-123', 'Laptop', 2, 1299.99, TIMESTAMP '2026-01-26 10:00:00')
   ```

6. **Spark Execution**
   - Spark receives SQL from service
   - Spark writes data as Parquet file to MinIO:
     - `s3://warehouse/sales/orders/data/00000-0-<uuid>.parquet`

7. **Iceberg Snapshot Creation**
   
   Iceberg performs these operations:

   a. **Create Manifest File** (Avro format)
      - Location: `s3://warehouse/sales/orders/metadata/<snap-id>-m0.avro`
      - Contains: List of data files with stats (record count, file size, column bounds, etc.)
      ```json
      [
        {
          "data_file": "s3://warehouse/sales/orders/data/00000-0-<uuid>.parquet",
          "record_count": 1,
          "file_size_in_bytes": 1024,
          "column_sizes": {...},
          "value_counts": {...}
        }
      ]
      ```

   b. **Create Manifest List** (Avro format)
      - Location: `s3://warehouse/sales/orders/metadata/snap-<snap-id>-1-<uuid>.avro`
      - Contains: List of manifest files
      ```json
      [
        {
          "manifest_path": "s3://warehouse/sales/orders/metadata/<snap-id>-m0.avro",
          "added_files_count": 1,
          "added_rows_count": 1
        }
      ]
      ```

   c. **Create New Metadata File**
      - Location: `s3://warehouse/sales/orders/metadata/v2.metadata.json`
      - Contains: Complete table metadata with new snapshot
      ```json
      {
        "format-version": 2,
        "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
        "location": "s3://warehouse/sales/orders",
        "last-updated-ms": 1738000000000,
        "current-snapshot-id": 8120351505078854920,
        "snapshots": [
          {
            "snapshot-id": 8120351505078854920,
            "timestamp-ms": 1738000000000,
            "manifest-list": "s3://warehouse/sales/orders/metadata/snap-8120351505078854920-1-<uuid>.avro",
            "summary": {
              "operation": "append",
              "added-records": "1",
              "total-records": "1",
              "added-data-files": "1",
              "total-data-files": "1"
            }
          }
        ],
        "schema": {...},
        "partition-spec": [...],
        "sort-orders": [...]
      }
      ```

8. **REST Catalog Update**
   - Iceberg sends REST API call to catalog: 
     ```
     POST http://localhost:8181/v1/namespaces/sales/tables/orders
     ```
   - Catalog updates its pointer to new metadata file:
     - Before: `s3://warehouse/sales/orders/metadata/v1.metadata.json`
     - After: `s3://warehouse/sales/orders/metadata/v2.metadata.json`
   - **Important:** Catalog stores ONLY the pointer, not the files themselves

9. **Response to Client**
   - Service retrieves snapshot ID from Iceberg table
   - Controller wraps snapshot ID in `CreateOrderResponse`
   - Spring Boot serializes to JSON and returns HTTP 201 Created

## Files Created in MinIO

After one INSERT operation, these files exist in MinIO:

```
s3://warehouse/
└── sales/
    └── orders/
        ├── data/
        │   └── 00000-0-a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d.parquet  # Actual data
        └── metadata/
            ├── v1.metadata.json                      # Initial metadata (table created)
            ├── v2.metadata.json                      # Updated metadata (after insert)
            ├── snap-8120351505078854920-1-x.avro    # Manifest list (snapshot)
            └── 8120351505078854920-m0.avro          # Manifest (data file list)
```

## Why REST Catalog Doesn't Store Data

The **REST Catalog** is a **metadata service**, not a storage system:

| Component | Stores What | Why |
|-----------|-------------|-----|
| **REST Catalog** | Table locations, schema pointers, namespace info | Lightweight, fast metadata queries |
| **MinIO (S3)** | Data files, manifests, metadata JSON | Durable, scalable, cheap storage |

**Analogy:** 
- REST Catalog = Library card catalog (tells you where books are)
- MinIO = Library shelves (stores the actual books)

## Iceberg Snapshot Benefits

Each write creates a **new snapshot** (immutable version):

1. **Time Travel**
   ```sql
   SELECT * FROM rest.sales.orders VERSION AS OF 8120351505078854920
   ```

2. **Rollback**
   ```sql
   CALL rest.system.rollback_to_snapshot('sales.orders', 8120351505078854920)
   ```

3. **ACID Transactions**
   - Atomicity: All-or-nothing writes
   - Consistency: Schema enforcement
   - Isolation: Readers never see partial writes
   - Durability: Metadata in durable storage (MinIO)

4. **Audit Trail**
   - Who wrote what data when
   - Complete history of table changes

## Configuration Explained

### application.properties

```properties
# REST Catalog connection
iceberg.catalog.uri=http://localhost:8181

# S3 (MinIO) configuration
iceberg.s3.endpoint=http://localhost:9000
iceberg.s3.access-key=admin
iceberg.s3.secret-key=password

# Spark + Iceberg integration
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.rest.type=rest
```

### Key Settings

- `spark.sql.extensions`: Adds Iceberg-specific SQL syntax (CREATE TABLE USING iceberg, etc.)
- `spark.sql.catalog.rest.type=rest`: Use REST protocol to talk to catalog
- `spark.sql.catalog.rest.io-impl=S3FileIO`: Use S3 for file I/O
- `s3.path-style-access=true`: Required for MinIO compatibility

## SQL Statements Used

1. **Create Namespace**
   ```sql
   CREATE NAMESPACE IF NOT EXISTS rest.sales
   ```

2. **Create Table**
   ```sql
   CREATE TABLE IF NOT EXISTS rest.sales.orders (
     order_id STRING,
     customer_id STRING,
     product_name STRING,
     quantity INT,
     price DECIMAL(10,2),
     order_timestamp TIMESTAMP
   ) USING iceberg
   ```

3. **Insert Data**
   ```sql
   INSERT INTO rest.sales.orders 
   VALUES ('ORD-001', 'CUST-123', 'Laptop', 2, 1299.99, TIMESTAMP '2026-01-26 10:00:00')
   ```

4. **Query Data**
   ```sql
   SELECT * FROM rest.sales.orders
   ```

5. **Time Travel Query**
   ```sql
   SELECT * FROM rest.sales.orders VERSION AS OF 8120351505078854920
   ```

## Testing

### 1. Test with Multiple Orders

```bash
# Order 1
curl -X POST http://localhost:8090/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-001", "customerId": "CUST-123", "productName": "Laptop", "quantity": 2, "price": 1299.99}'

# Order 2
curl -X POST http://localhost:8090/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "ORD-002", "customerId": "CUST-456", "productName": "Mouse", "quantity": 5, "price": 29.99}'
```

Each order creates a **new snapshot**.

### 2. Verify in MinIO

1. Open MinIO UI: http://localhost:9001
2. Login: admin / password
3. Navigate to `warehouse` bucket
4. Browse `sales/orders/` to see:
   - `data/` folder with Parquet files
   - `metadata/` folder with versioned metadata JSON files

### 3. Query with Spark SQL (via Docker)

```bash
# Connect to Spark container
docker exec -it spark-iceberg spark-sql

# Query the table
spark-sql> SELECT * FROM rest.sales.orders;
```

## Troubleshooting

### Issue: Cannot connect to REST Catalog

**Solution:** Ensure REST Catalog is running:
```bash
docker ps | grep iceberg-rest
curl http://localhost:8181/v1/config
```

### Issue: Cannot connect to MinIO

**Solution:** Ensure MinIO is running:
```bash
docker ps | grep minio
curl http://localhost:9000/minio/health/live
```

### Issue: Spring Boot fails to start

**Solution:** Check if port 8090 is available:
```bash
lsof -i :8090
```

## Advanced Topics

### Schema Evolution

Add a new column without rewriting data:
```sql
ALTER TABLE rest.sales.orders ADD COLUMN discount DECIMAL(5,2)
```

### Hidden Partitioning

Partition by date (hidden from users):
```sql
CREATE TABLE rest.sales.orders (
  order_id STRING,
  customer_id STRING,
  product_name STRING,
  quantity INT,
  price DECIMAL(10,2),
  order_timestamp TIMESTAMP
) USING iceberg
PARTITIONED BY (days(order_timestamp))
```

### Table Maintenance

```sql
-- Expire old snapshots (data cleanup)
CALL rest.system.expire_snapshots('sales.orders', TIMESTAMP '2026-01-20 00:00:00')

-- Rewrite small files into larger ones
CALL rest.system.rewrite_data_files('sales.orders')
```

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Iceberg REST Catalog Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [Spring Boot Documentation](https://spring.io/projects/spring-boot)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)

---

## Tóm Tắt Các Thay Đổi Đã Thực Hiện

### Files Mới Được Tạo:
1. **UpdateOrderRequest.java** - DTO cho UPDATE request
2. **OrderResponse.java** - DTO cho GET/TIME TRAVEL responses
3. **SnapshotInfo.java** - DTO cho snapshot metadata
4. **BatchOrderRequest.java** - DTO cho batch ingestion
5. **ExpireSnapshotsByTimeRequest.java** - DTO cho expire by time
6. **ExpireSnapshotsByCountRequest.java** - DTO cho expire by count
7. **ExpireSnapshotsHybridRequest.java** - DTO cho expire hybrid strategy
8. **ExpireSnapshotsResponse.java** - DTO cho expire snapshots response

### Files Đã Được Cập Nhật:

#### OrderRepository.java
- Thêm `batchInsertOrders(List<Map<String, Object>> orders)` - Interface cho batch insert
- Thêm `expireSnapshotsByTime(String olderThanTimestamp)` - Xóa snapshots theo thời gian UTC
- Thêm `expireSnapshotsByCount(int retainLast)` - Xóa snapshots giữ lại N snapshots
- Thêm `expireSnapshotsHybrid(String olderThanTimestamp, int retainLast)` - Xóa theo thời gian nhưng giữ số lượng

#### SparkOrderRepository.java
- Implement `batchInsertOrders()` - Sử dụng Spark DataFrame API cho batch insert
- Implement `expireSnapshotsByTime()` - Gọi Iceberg system procedure expire_snapshots theo timestamp
- Implement `expireSnapshotsByCount()` - Tính toán và expire giữ lại N snapshots mới nhất
- Implement `expireSnapshotsHybrid()` - Kết hợp time-based và count-based cleanup

#### OrderService.java
Đã thêm methods:
- `batchIngestOrders(request)` - Batch ingestion với transaction duy nhất
- `expireSnapshotsByTime(olderThanTimestamp)` - Service layer cho expire by time
- `expireSnapshotsByCount(retainLast)` - Service layer cho expire by count
- `expireSnapshotsHybrid(olderThanTimestamp, retainLast)` - Service layer cho hybrid strategy
- `getAllOrdersAsList()` - Lấy tất cả orders dạng List
- `updateOrder(orderId, request)` - Cập nhật order, trả về snapshot ID
- `deleteOrder(orderId)` - Xóa order, trả về snapshot ID
- `getSnapshotHistory()` - Lấy lịch sử tất cả snapshots
- `getOrdersAtSnapshot(snapshotId)` - Time travel theo snapshot ID
- `getOrdersAtTimestamp(timestamp)` - Time travel theo timestamp
- `queryOrders(request)` - Query với filters phức tạp
- `mergeOrder(request)` - Upsert operation

#### OrderController.java
Đã thêm REST endpoints:
- `POST /orders/batch` - Batch ingestion
- `POST /orders/snapshots/expire/by-time` - Expire snapshots theo UTC timestamp
- `POST /orders/snapshots/expire/by-count` - Expire snapshots giữ lại N snapshots
- `POST /orders/snapshots/expire/hybrid` - Expire snapshots hybrid strategy
- `GET /orders` - Lấy tất cả orders
- `PUT /orders/{orderId}` - Cập nhật order
- `DELETE /orders/{orderId}` - Xóa order
- `GET /orders/snapshots` - Xem snapshot history
- `GET /orders/time-travel/snapshot/{snapshotId}` - Time travel by snapshot
- `GET /orders/time-travel/timestamp/{timestamp}` - Time travel by timestamp
- `POST /orders/query` - Query với filters
- `POST /orders/merge` - Merge/upsert operation

#### README.md
- Cập nhật overview với batch ingestion và expire snapshots
- Thêm documentation cho 3 APIs expire snapshots
- Thêm ví dụ UTC timestamp format
- Giải thích chi tiết về các strategies: by-time, by-count, hybrid

### Tính Năng Đã Hoàn Thành:
✅ **INSERT** - Tạo order mới với Iceberg snapshot
✅ **BATCH INGESTION** - Thêm hàng nghìn records trong 1 transaction
✅ **UPDATE** - Cập nhật order với versioning
✅ **DELETE** - Xóa order (soft delete với snapshot)
✅ **MERGE** - Upsert operation (insert hoặc update)
✅ **QUERY** - Lọc dữ liệu với điều kiện phức tạp
✅ **SNAPSHOT HISTORY** - Xem lịch sử tất cả snapshots
✅ **TIME TRAVEL** - Query dữ liệu tại bất kỳ thời điểm nào
✅ **EXPIRE SNAPSHOTS BY TIME** - Xóa snapshots theo UTC timestamp
✅ **EXPIRE SNAPSHOTS BY COUNT** - Xóa snapshots giữ lại N snapshots mới nhất
✅ **EXPIRE SNAPSHOTS HYBRID** - Xóa theo time nhưng đảm bảo giữ số lượng tối thiểu

### Cách Chạy:
1. Start infrastructure: `docker-compose up -d`
2. Build project: `mvn clean install`
3. Run application: `mvn spring-boot:run`

### Test Batch Ingestion:
```bash
curl -X POST http://localhost:8090/orders/batch \
  -H "Content-Type: application/json" \
  -d '{
    "orders": [
      {"orderId": "ORD-001", "customerId": "CUST-001", "productName": "Laptop", "quantity": 2, "price": 1299.99},
      {"orderId": "ORD-002", "customerId": "CUST-002", "productName": "Mouse", "quantity": 10, "price": 29.99}
    ]
  }'
```

### Test Expire Snapshots:
```bash
# Xóa theo thời gian
curl -X POST http://localhost:8090/orders/snapshots/expire/by-time \
  -H "Content-Type: application/json" \
  -d '{"olderThanTimestamp": "2026-01-25 00:00:00"}'

# Xóa giữ lại N snapshots
curl -X POST http://localhost:8090/orders/snapshots/expire/by-count \
  -H "Content-Type: application/json" \
  -d '{"retainLast": 5}'

# Hybrid: xóa theo time nhưng giữ số lượng
curl -X POST http://localhost:8090/orders/snapshots/expire/hybrid \
  -H "Content-Type: application/json" \
  -d '{"olderThanTimestamp": "2026-01-22 00:00:00", "retainLast": 3}'
```
3. Run application: `mvn spring-boot:run`
4. Test endpoints theo hướng dẫn trong README

## License

MIT
