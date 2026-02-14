build-base:
	docker build -f Dockerfile.base -t worker-base:latest . 
build-download:
	docker build -f Dockerfile.download -t worker-downlaod:latest . 
build-audio:
	docker build -f Dockerfile.audio -t worker-audio:latest . 
build-transcribe:
	docker build -f Dockerfile.transcribe -t worker-transcribe:latest . 
build: build-base build-download build-audio build-transcribe
	echo "done"