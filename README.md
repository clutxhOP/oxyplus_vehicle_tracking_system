# Advanced GPS Tracking & Logistics Platform

## Table of Contents
- [Overview](#overview)
- [System Architecture](#system-architecture)
- [Quick Start](#quick-start)
- [System Components](#system-components)
- [Configuration](#configuration)
- [API Documentation](#api-documentation)
- [Data Flow](#data-flow)
- [Monitoring & Alerts](#monitoring--alerts)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Overview

This is an enterprise-grade fleet management system designed for automated logistics operations. The platform combines GPS tracking, route optimization, intelligent alerting, and AI-powered customer acquisition through a unified web interface.

### Key Features
- **Real-time Fleet Tracking**: Monitor vehicle locations and activities
- **Intelligent Alert System**: Automated notifications for operational issues
- **Interactive Mapping**: Visual route analysis and customer management
- **WhatsApp Bot Integration**: Automated customer acquisition
- **AI-Powered Analytics**: Natural language business intelligence
- **Comprehensive Reporting**: Historical data analysis and insights

### Technology Stack
- **Backend**: Python, Flask, Pandas, SQLite
- **Frontend**: HTML, CSS, JavaScript, Folium
- **Automation**: Selenium, undetected-chromedriver
- **AI/ML**: OpenAI GPT-4, Google Gemini
- **APIs**: OpenRouteService, WhatsApp Business API
- **Scheduling**: Python schedule library

## System Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │────│  Data Pipeline  │────│   Web App       │
│  (TouchTraks)   │    │  (Extract/ETL)  │    │   (Flask)       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │              ┌─────────────────┐              │
         │              │   Alert System  │              │
         └──────────────│   (WhatsApp)    │──────────────┘
                        └─────────────────┘
                                 │
                        ┌─────────────────┐
                        │   WhatsApp Bot  │
                        │  (Customer Acq) │
                        └─────────────────┘
```

## Quick Start

### Prerequisites
- Python 3.8+
- Chrome browser installed
- Internet connection for API access

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd fleet-management-system
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the system**
   ```bash
   # Copy example configuration
   cp config_data/example_settings.json config_data/app_settings.json
   
   # Edit configuration with your API keys and credentials
   nano config_data/app_settings.json
   ```

4. **Start the system**
   ```bash
   python master.py
   ```

5. **Access the web interface**
   - Open browser to `http://localhost:5000`
   - Use configured credentials to log in

### Essential Configuration

Edit `config_data/app_settings.json`:

```json
{
  "ors_api_key": "your_openrouteservice_api_key",
  "gemini_api_key": "your_google_gemini_api_key",
  "openai_api_key": "your_openai_api_key",
  "whatsapp_server_url": "your_whatsapp_api_endpoint",
  "alert_followup_url": "your_whatsapp_webhook_url"
}
```

## System Components

### 1. Master Orchestrator (`master.py`)

The central control system that manages all subsystems:

- **Web Server Management**: Starts and monitors Flask application
- **Job Scheduling**: Manages all recurring tasks
- **System Lifecycle**: Handles startup, shutdown, and recovery
- **Process Coordination**: Ensures proper execution order

**Key Functions:**
```python
def data_extraction_and_formatting_job()  # Every 15 minutes
def alert_monitoring_job()                # Every 10 minutes  
def whatsapp_script_job()                 # Every 30 minutes
def daily_restart_job()                   # Daily at 03:00
```

### 2. Data Extraction System (`extractdata.py`)

Automated data collection from GPS provider:

- **Browser Automation**: Uses undetected Chrome for stealth scraping
- **Multi-Report Download**: Fetches 5 different report types
- **Error Recovery**: Robust retry mechanisms
- **Process Cleanup**: Ensures no zombie processes

**Supported Reports:**
- Travel Report (XML): Movement and location data
- Driver Performance (XML): Driving behavior metrics
- Geofence Report (XLSX): Zone entry/exit logs
- Idle Report (XLSX): Engine idle tracking
- Excess Idle Report (XLSX): Extended idle periods

### 3. Data Processing (`formatdata.py`)

ETL pipeline for raw data transformation:

- **Format Standardization**: Converts XML/XLSX to CSV
- **Data Cleaning**: Removes duplicates and inconsistencies
- **Time-based Partitioning**: Separates current vs historical data
- **Schema Validation**: Ensures data integrity

### 4. Web Application (`app.py`)

Flask-based dashboard and API server:

**Main Routes:**
- `/`: Dashboard overview
- `/weekly-customers`: Interactive customer mapping
- `/daily`: Route comparison analysis
- `/chat`: AI-powered query interface
- `/settings`: System configuration
- `/api/*`: RESTful API endpoints

**Key Features:**
- Auth0 integration for secure access
- Real-time map visualization with Folium
- Dynamic filtering and analysis tools
- Mobile-responsive design

### 5. Alert System

Proactive monitoring with intelligent notifications:

**Alert Types:**
- **Idle Alerts**: Extended vehicle idle time
- **Geofence Violations**: Unauthorized location visits
- **Performance Issues**: Harsh driving behaviors
- **Route Deviations**: Off-track vehicle movements
- **Early Returns**: Incomplete route execution

**Alert Flow:**
```
Data Change → Condition Check → Alert Generation → De-duplication → WhatsApp Delivery
```

### 6. WhatsApp Bot System (`whatsappbot/`)

Automated customer acquisition pipeline:

**Process Flow:**
1. **Contact Ingestion**: Reads prospect lists
2. **Initial Outreach**: Automated greeting messages
3. **Information Collection**: AI-powered name and location extraction
4. **Data Integration**: Seamless CRM integration
5. **Follow-up Management**: Intelligent conversation handling

### 7. RAG Analytics System (`utils.py`)

AI-powered business intelligence:

- **Natural Language Processing**: Converts questions to data queries
- **Context Retrieval**: Fetches relevant historical data
- **Response Generation**: Creates data-driven answers
- **Conversation Memory**: Maintains context across interactions

## Configuration

### Primary Configuration Files

1. **`config_data/app_settings.json`** - Main system settings
2. **`config_data/credentials.json`** - GPS provider login credentials
3. **`config_data/phone_no.json`** - Alert recipient phone numbers
4. **`config_data/vehicle_aliases.json`** - Vehicle ID to driver name mapping

### Environment Variables

```bash
export FLASK_ENV=production
export AUTH0_CLIENT_ID=your_auth0_client
export AUTH0_CLIENT_SECRET=your_auth0_secret
export AUTH0_DOMAIN=your_auth0_domain
```

### API Keys Required

1. **OpenRouteService**: Route calculation and optimization
2. **Google Gemini**: AI analytics and natural language processing
3. **OpenAI GPT-4**: WhatsApp bot conversation intelligence
4. **WhatsApp Business API**: Message delivery service
5. **Auth0**: User authentication and authorization

## API Documentation

### Core Endpoints

#### Vehicle Data
```http
GET /api/vehicles
GET /api/vehicles/{vehicle_id}/status
GET /api/vehicles/{vehicle_id}/history?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
```

#### Customer Management
```http
GET /api/customers
POST /api/customers
PUT /api/customers/{customer_id}
DELETE /api/customers/{customer_id}
```

#### Alert System
```http
GET /api/alerts?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
POST /api/alerts/test
GET /api/alerts/config
```

#### Analytics
```http
POST /api/chat
Content-Type: application/json
{
  "message": "How many deliveries were completed yesterday?",
  "session_id": "user_session_123"
}
```

### Response Formats

All API responses follow this structure:
```json
{
  "success": true,
  "data": {...},
  "message": "Operation completed successfully",
  "timestamp": "2025-01-31T10:30:00Z"
}
```

## Data Flow

### 1. Data Ingestion Pipeline

```
GPS Provider → Selenium Scraper → Raw Files → ETL Process → Processed CSV → Database
```

### 2. Alert Processing Pipeline

```
CSV Data → Condition Checkers → Alert Generator → De-duplicator → WhatsApp API → Delivery
```

### 3. Web Interface Data Flow

```
User Request → Flask Route → Data Query → Processing → Template Rendering → Response
```

### 4. WhatsApp Bot Flow

```
Contact List → Message Sender → Response Listener → AI Processor → Data Extractor → CRM Update
```

## Monitoring & Alerts

### System Health Monitoring

The platform includes comprehensive monitoring:

- **Process Health**: Monitors all background processes
- **Data Freshness**: Ensures regular data updates
- **API Connectivity**: Validates external service connections
- **Disk Space**: Monitors storage utilization
- **Memory Usage**: Tracks system resource consumption

### Alert Configuration

Customize alert thresholds in settings:

```json
{
  "idle_threshold_minutes": 20,
  "violation_threshold": 3,
  "route_deviation_threshold": 500,
  "early_return_time": "16:00"
}
```

### Log Files

System logs are maintained in:
- `logs/master.log`: Main orchestration logs
- `logs/extraction.log`: Data scraping activities
- `logs/alerts.log`: Alert system activities
- `logs/webapp.log`: Web application logs
- `whatsappbot/logs/`: WhatsApp bot conversation logs

## Troubleshooting

### Common Issues

#### 1. Data Extraction Fails
```bash
# Check Chrome browser installation
google-chrome --version

# Verify credentials file
cat config_data/credentials.json

# Check extraction logs
tail -f logs/extraction.log
```

#### 2. WhatsApp Alerts Not Sending
```bash
# Test WhatsApp API connectivity
curl -X POST "your_whatsapp_api_url/test"

# Check phone number configuration
cat config_data/phone_no.json

# Review alert logs
tail -f logs/alerts.log
```

#### 3. Web Application Not Loading
```bash
# Check Flask process
ps aux | grep python

# Verify port availability
netstat -tlnp | grep :5000

# Check web application logs
tail -f logs/webapp.log
```

#### 4. AI Chat Not Responding
```bash
# Verify API keys
grep -i "gemini\|openai" config_data/app_settings.json

# Test API connectivity
python -c "import openai; print('OpenAI OK')"

# Check chat logs
grep "chat" logs/webapp.log
```

### Debug Mode

Enable detailed logging:

```bash
export FLASK_DEBUG=1
export LOG_LEVEL=DEBUG
python master.py
```

### Reset System State

```bash
# Clear all processed data (keeps raw backups)
rm -rf data/*/current.csv
rm -rf data/*/history.csv

# Reset WhatsApp bot state
python whatsappbot/clean.py

# Restart all processes
python master.py --reset
```

## Performance Optimization

### Database Optimization
- Regular cleanup of old data files
- Index optimization for faster queries
- Batch processing for large datasets

### Memory Management
- Automatic conversation history cleanup
- Efficient DataFrame operations
- Background process memory monitoring

### Network Optimization
- API request rate limiting
- Retry mechanisms with exponential backoff
- Connection pooling for external services

## Security Considerations

### Data Protection
- All sensitive data is encrypted at rest
- API keys stored in secure configuration files
- User authentication through Auth0
- Regular security updates and patches

### Access Control
- Role-based permissions system
- Secure session management
- API endpoint protection
- Audit logging for all operations

## Contributing

### Development Setup

1. **Create development environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   # or
   venv\Scripts\activate     # Windows
   ```

2. **Install development dependencies**
   ```bash
   pip install -r requirements-dev.txt
   ```

3. **Run tests**
   ```bash
   python -m pytest tests/
   ```

### Code Style

- Follow PEP 8 style guidelines
- Use type hints where possible
- Include docstrings for all functions
- Write unit tests for new features

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Update documentation
6. Submit pull request

## License

This project is proprietary software developed for OxyPlus Water. All rights reserved.

## Support

For technical support or questions:
- Create an issue in the project repository
- Contact the development team
- Refer to the troubleshooting section above

---

**Last Updated:** January 2025  
**Version:** 2.0.0  
**Platform Compatibility:** Linux, Windows, macOS