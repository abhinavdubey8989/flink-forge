## Purpose of this package

- It contains utils, DTO etc. which are used across the other flink jobs
- `mapper` is for conversion b/w kafka byte[] & Java DTO
- `dto` has the DTOs
- `config` has util related to reading configs
- `env` has the business logic to get the default Flink execution env
- `source` has the business logic to get the default Flink kafka source