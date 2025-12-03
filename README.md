# 🧠 Psychology Session Analyzer

An event-driven microservices system that automatically analyzes video recordings of therapy sessions. It utilizes **OpenAI** for psychological insights, **AssemblyAI** for speaker diarization, and a robust pipeline of **Dockerized** services orchestrated via **RabbitMQ**.

---

## 🏗 System Architecture

This system follows a **Pipeline Architecture** where data flows through a series of decoupled microservices. Services communication is asynchronous, using **RabbitMQ** as the message broker.

### Data Flow Diagram
```mermaid
graph TD
    User((User)) -->|POST /upload| UploadService[Upload Service]
    
    subgraph Infrastructure
        MinIO[(MinIO Storage)]
        RabbitMQ{RabbitMQ}
        Redis[(Redis Cache)]
        MongoDB[(MongoDB)]
    end

    UploadService -->|Save Video| MinIO
    UploadService -->|Pub: video_uploaded| RabbitMQ

    RabbitMQ -->|Sub: video_uploaded| ConverterService[Converter Service]
    ConverterService -->|Get Video| MinIO
    ConverterService -->|Extract Audio| ConverterService
    ConverterService -->|Save MP3| MinIO
    ConverterService -->|Pub: audio_ready| RabbitMQ

    RabbitMQ -->|Sub: audio_ready| TranscriptionService[Transcription Service]
    TranscriptionService -->|External API| AssemblyAI[AssemblyAI]
    TranscriptionService -->|Pub: transcription_ready| RabbitMQ

    RabbitMQ -->|Sub: transcription_ready| AnalysisService[Analysis Service]
    AnalysisService -->|Check Hash| Redis
    AnalysisService -->|External API| OpenAI[OpenAI GPT-4]
    AnalysisService -->|Save Analysis| MongoDB

    User -->|GET /analyses| QueryService[Query Service]
    QueryService -->|Read| MongoDB
