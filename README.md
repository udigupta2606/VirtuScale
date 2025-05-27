# VirtuScale: Leveraging Virtual Memory for Scalable & Adaptive Memory Management in Modern OS

VirtuScale is a high-performance, lock-safe memory management system combining custom virtual memory caching (vmcache) with Linux's /dev/exmap kernel module for precise, user-space page-level control. Designed for data-intensive systems, VirtuScale enables efficient memory residency tracking, eviction, concurrency handling, and fault recovery using signal-based mechanisms.

🚀 Features
🧠 Custom Buffer Manager (vmcache) with page-level control, eviction, and reload

🔐 Concurrency-safe Guards (GuardO, GuardS, GuardX) for automatic locking

🌳 In-memory B-Tree Engine supporting transactional insert/delete/split

⚡ /dev/exmap Integration for kernel-assisted memory residency tracking

🧩 Signal-based Page Fault Recovery using SIGSEGV handling

📈 Benchmarking Suite with performance comparison (vmcache vs vmcache + exmap)

🔧 CLI Tools for runtime monitoring and control

🧩 Architecture Overview
Key Components:

PageState: Tracks lock versions and access state

ResidentPageSet: Monitors page residency in RAM

BufferManager: Manages page allocation, eviction, and reload

GuardO/S/X: RAII wrappers for page access control

BTree & BTreeNode: Memory-efficient record storage

vmcacheAdapter: BTree-to-typed-record mapper

SIGSEGV Handler: Reloads evicted pages on fault

/dev/exmap: Enables direct user-space physical memory mapping via ioctl

📊 Benchmarks
Simulated workloads (TPC-C, random read) were used to evaluate:

Page fault recovery latency

Eviction performance

Throughput under load

vmcache vs vmcache + exmap comparison

✅ Tests Passed
Test Type	Status	Description
Unit Test – Locking	✅ Pass	Validated all lock transitions
Guards Lifecycle	✅ Pass	RAII-based access with Guard wrappers
Signal Handling	✅ Pass	Reloads evicted pages via SIGSEGV logic
Integration Tests	✅ Pass	Page eviction and reload validation
TPC-C Simulation	✅ Pass	Full transaction suite
Exmap Integration	✅ Pass	Outperformed standalone vmcache


👨‍💻 Team
Suhawni Arora, Udi Gupta, Gurpreet Singh, Ayush Raturi

📌 Future Work
NUMA-aware eviction policies

Cross-platform memory APIs (Windows/macOS support)

Workload-aware adaptive eviction strategies

Advanced security & access control

Real-time visualization dashboards for memory metrics

📎 License
This project is part of TCS-611 Software Engineering (2025) and for educational purposes. Licensing terms will be finalized post-evaluation.

