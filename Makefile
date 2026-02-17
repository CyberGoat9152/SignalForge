build-base:
	docker build -f Dockerfile.base -t worker-base:latest . 
build-download:
	docker build -f Dockerfile.download -t worker-download:latest . 
build-audio:
	docker build -f Dockerfile.audio -t worker-audio:latest . 
build-transcribe:
	docker build -f Dockerfile.transcribe -t worker-transcribe:latest . 
build-streamlit:
	docker build -f Dockerfile.streamlit -t worker-streamlit:latest . 
build: build-base build-download build-audio build-streamlit build-transcribe
	@echo "done"