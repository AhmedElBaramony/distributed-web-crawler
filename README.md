# 🌐 Distributed Web Crawling & Indexing System

This project implements a **scalable**, **fault-tolerant**, and **cloud-deployable** system for web crawling and keyword-based indexing using Python and distributed computing principles. It is designed to handle large-scale crawling tasks by distributing workloads across multiple crawler and indexer nodes running in the cloud.

---

## 📌 Features

- ✅ Distributed web crawling using MPI (`mpi4py`)
- ✅ Lightweight HTML fetching and parsing (`requests` + `BeautifulSoup4`)
- ✅ Searchable keyword index powered by `Whoosh`
- ✅ Fault-tolerant task handling (node failure recovery via master node)
- ✅ Cloud-compatible: tested with GCP & AWS VMs
- ✅ Persistent cloud storage for crawled data and indexes
- ✅ Search interface for querying indexed pages (basic CLI/web)

---

## 🛠️ Technology Stack

| Component         | Choice                        | Reason                                                             |
|------------------|-------------------------------|--------------------------------------------------------------------|
| **Language**      | Python                        | Easy syntax, fast prototyping, team familiarity                    |
| **Crawling**      | `requests` + `BeautifulSoup4` | Lightweight, flexible, beginner-friendly                           |
| **Distribution**  | `mpi4py`                      | Works without brokers, ideal for local and cloud parallelism       |
| **Indexing**      | `Whoosh`                      | Pure Python, no external services, easy integration                |
| **Storage**       | GCP Cloud Storage (+AWS opt.) | Distributed, persistent, free-tier compatible                      |
| **Cloud Platform**| GCP + AWS                     | Combines free tiers for cost-free multi-VM deployment              |
| **Version Control**| Git + GitHub                 | Team collaboration, versioning, tracking changes                   |

---

## 🗂️ Project Structure

```
distributed-web-crawler/
├── master_node/          # Master node for scheduling and task distribution
├── crawler_node/         # Web crawling logic
├── indexer_node/         # Index creation and query handling
├── shared/               # Utility modules and shared logic (optional)
├── docs/                 # Reports, architecture diagrams, setup guides
├── requirements.txt      # Python dependencies
├── .gitignore            # Files and folders excluded from version control
└── README.md             # You're reading it :)
```

---

## 🚀 How to Run (Mac & Windows)

### 🔧 1. Clone the Repository

```bash
git clone https://github.com/your-username/distributed-web-crawler.git
cd distributed-web-crawler
```

### 🐍 2. Set Up Virtual Environment (Recommended)

```bash
python3 -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
```

### 📦 3. Install Dependencies

Make sure MPI is installed first (see below), then:

```bash
pip install -r requirements.txt
```

### 🧠 4. Install MPI (macOS)

```bash
brew install mpich               # Or: brew install open-mpi
```

Then install `mpi4py`:

```bash
MPICC=/opt/homebrew/bin/mpicc pip install mpi4py
```

### 🧪 5. Run with MPI (Example: 1 master, 2 crawlers, 1 indexer)

```bash
mpiexec -n 4 python master_node/master_node.py
```

> Adjust the script and rank logic in your code to match the roles.

---

## 💡 How It Works

- The **Master Node** distributes crawling tasks and monitors worker health
- **Crawler Nodes** fetch web pages, extract text and links, and send results back
- **Indexer Nodes** build an inverted index for keyword searching
- **Cloud Storage** holds crawled HTML, parsed data, and indexes
- Fault tolerance is handled through heartbeats and task reassignment

---

## 👨‍💻 Team Members

| Name     | Role                  | OS        |
|----------|-----------------------|-----------|
| Ahmed    | Project Architect     | macOS     |
| Sara     | Crawler Development   | Windows   |
| Youssef  | Cloud Infrastructure  | Windows   |
| Fatma    | Indexer & QA Lead     | Windows   |

---

## 📄 License

This project is part of Ain Shams University’s CSE354 Distributed Computing course – Spring 2025.

---

## 🤝 Contribution Guidelines

- Branch from `main` using feature branches (e.g., `feature/crawler`)
- Use clear commit messages and open pull requests
- Add tests and docstrings for any shared functions or modules
