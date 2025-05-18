# 🌐 Distributed Web Crawling & Indexing System

This project implements a **scalable**, **fault-tolerant**, and **cloud-deployable** system for web crawling and keyword-based indexing using Python and AWS services. The system is deployed on multiple EC2 instances to achieve true distributed computing capabilities, with AWS SQS (Simple Queue Service) handling the message-based communication between nodes.

---

## 📌 Features

- ✅ Distributed web crawling using AWS SQS for message-based communication
- ✅ Multi-node deployment on AWS EC2 instances
- ✅ Lightweight HTML fetching and parsing (`requests` + `BeautifulSoup4`)
- ✅ Searchable keyword index powered by `Whoosh`
- ✅ Fault-tolerant task handling with SQS message visibility timeout
- ✅ Cloud-native architecture using AWS services
- ✅ Persistent cloud storage for crawled data and indexes
- ✅ Real-time dashboard for monitoring crawling progress
- ✅ Search interface for querying indexed pages (web-based)
- ✅ Comprehensive logging and monitoring system
- ✅ Auto-scaling capabilities through EC2 instance management

---

## 🛠️ Technology Stack

| Component         | Choice                        | Reason                                                             |
|------------------|-------------------------------|--------------------------------------------------------------------|
| **Language**      | Python                        | Easy syntax, fast prototyping, team familiarity                    |
| **Crawling**      | `requests` + `BeautifulSoup4` | Lightweight, flexible, beginner-friendly                           |
| **Message Queue** | AWS SQS                       | Reliable, scalable message-based communication                     |
| **Cloud Compute** | AWS EC2                       | Flexible, scalable virtual machines for distributed processing     |
| **Indexing**      | `Whoosh`                      | Pure Python, no external services, easy integration                |
| **Storage**       | AWS S3                        | Highly available, durable object storage                           |
| **Web Framework** | Flask                         | Lightweight, easy to integrate with existing Python code           |
| **Version Control**| Git + GitHub                 | Team collaboration, versioning, tracking changes                   |

---

## 📈 Project Phases

### Phase 1: Basic Implementation
- Initial implementation of master, crawler, and indexer nodes
- Basic crawling functionality with single-node architecture
- Simple text extraction and storage
- Foundation for distributed architecture

### Phase 2: Enhanced Crawling & Indexing
- Improved crawler with better URL handling
- Enhanced indexer with CSV-based storage
- Added search functionality
- Basic error handling and logging

### Phase 3: Distributed Computing Implementation
#### Phase 3A: MPI Implementation
- Distributed crawling using MPI
- Parallel processing across multiple nodes
- Enhanced master node coordination
- Improved logging system
- Index directory management

#### Phase 3B: AWS SQS Implementation
- Message queue-based distribution using AWS SQS
- Cloud-ready architecture
- Real-time dashboard for monitoring
- Enhanced fault tolerance
- Improved task distribution

### Phase 4: Final Implementation (Current)
- Production-ready SQS-based distributed system
- Multi-node deployment on AWS EC2 instances
- Enhanced web dashboard with real-time metrics
- Improved error handling and recovery
- Advanced logging system
- Optimized performance
- Better resource management
- Auto-scaling capabilities

---

## 🗂️ Project Structure

```
distributed-web-crawler/
├── Phase_1/              # Basic implementation
│   ├── master_node.py
│   ├── crawler_node.py
│   └── indexer_node.py
├── Phase_2/              # Enhanced implementation
│   ├── master_node.py
│   ├── crawler_node.py
│   ├── indexer_node.py
│   └── search_index.py
├── Phase_3_MPI/          # MPI-based distribution
│   ├── master_node.py
│   ├── crawler_node.py
│   ├── indexer_node.py
│   └── Indexing Tools/
├── Phase_3_SQS/          # Initial SQS implementation
│   ├── master_node.py
│   ├── crawler_node.py
│   ├── indexer_node.py
│   ├── dashboard.py
│   └── templates/
├── Phase_4/              # Current production implementation
│   ├── master_node.py    # SQS-based master node
│   ├── crawler_node.py   # EC2-deployed crawler
│   ├── indexer_node.py   # EC2-deployed indexer
│   ├── app.py           # Web dashboard
│   ├── sqs_config.py    # AWS SQS configuration
│   ├── dashboard_logger.py
│   └── templates/       # Web interface templates
├── requirements.txt      # Python dependencies
├── .gitignore           # Files and folders excluded from version control
└── README.md            # Project documentation
```

---

## 🚀 How to Run

### 🔧 1. Clone the Repository

```bash
git clone https://github.com/your-username/distributed-web-crawler.git
cd distributed-web-crawler
```

### 🐍 2. Set Up Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate        # On Windows: .venv\Scripts\activate
```

### 📦 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 🔐 4. AWS Configuration

1. Set up AWS credentials:
```bash
aws configure
```

2. Create required AWS resources:
   - SQS queues for task distribution
   - EC2 instances for nodes
   - S3 bucket for storage
   - IAM roles and policies

### 🧪 5. Running the Current Implementation (Phase 4)

#### Local Development
```bash
python Phase_4/app.py            # Starts the web interface
python Phase_4/master_node.py    # Starts the master node
```

#### EC2 Deployment
1. Launch EC2 instances:
   - Master node: t2.micro (1 instance)
   - Crawler nodes: t2.micro (multiple instances)
   - Indexer nodes: t2.micro (multiple instances)

2. Configure security groups:
   - Allow HTTP/HTTPS traffic
   - Allow inter-node communication
   - Restrict access to necessary ports

3. Deploy code to instances:
```bash
# On each EC2 instance
git clone <repository-url>
cd distributed-web-crawler
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

4. Start services:
```bash
# On master node
python Phase_4/master_node.py

# On crawler nodes
python Phase_4/crawler_node.py

# On indexer nodes
python Phase_4/indexer_node.py

# On dashboard node
python Phase_4/app.py
```

---

## 💡 How It Works

1. **Master Node (EC2)**
   - Coordinates crawling tasks through SQS
   - Monitors worker health
   - Handles task distribution
   - Manages fault tolerance
   - Communicates with all nodes via SQS queues

2. **Crawler Nodes (EC2)**
   - Fetch web pages
   - Extract text and links
   - Send results to indexer via SQS
   - Handle rate limiting
   - Auto-recover from failures

3. **Indexer Nodes (EC2)**
   - Build inverted index
   - Store indexed data in S3
   - Handle search queries
   - Manage index updates
   - Process messages from SQS

4. **Web Interface (EC2)**
   - Real-time monitoring
   - Search functionality
   - System statistics
   - Progress tracking
   - Node status monitoring

5. **AWS SQS Integration**
   - Task distribution queue
   - Results processing queue
   - Indexing queue
   - Message visibility timeout for fault tolerance
   - Dead-letter queues for error handling

---

## 👨‍💻 Team Members

| Name                           | Role                                         |
|--------------------------------|----------------------------------------------|
| Ahmed Ehab Mohamed El-Baramony | Crawler Lead, Tester/Documentation Lead      |
| Ahmed Mohamed Mohamed          | Architect                                    |
| Ahmed Mohamed El-Henawy        | Cloud Infrastructure Lead                    |
| Mohamed Hassan                 | Indexer                                      |

---

## 📄 License

This project is part of Ain Shams University's CSE354 Distributed Computing course – Spring 2025.

---

## 🤝 Contribution Guidelines

- Branch from `main` using feature branches (e.g., `feature/crawler`)
- Use clear commit messages and open pull requests
- Add tests and docstrings for any shared functions or modules
- Follow the established code style and architecture
- Update documentation for any significant changes

---

## 🔍 Future Improvements

- [ ] Implement advanced caching mechanisms
- [ ] Add support for more file formats
- [ ] Enhance distributed processing capabilities
- [ ] Improve search algorithm efficiency
- [ ] Add more monitoring metrics
- [ ] Implement advanced security features
- [ ] Add auto-scaling based on queue size
- [ ] Implement cost optimization strategies
- [ ] Add support for more AWS services
- [ ] Enhance dashboard with more metrics
