FROM apache/beam_python3.12_sdk:2.59.0

ENV HF_HOME=/opt/hf_cache \
    PIP_NO_CACHE_DIR=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Tải sẵn 2 model vào image
RUN python -c "from huggingface_hub import snapshot_download as d; \
d('NlpHUST/ner-vietnamese-electra-base'); \
d('wonrax/phobert-base-vietnamese-sentiment')"

# Worker không tải lại từ internet nữa
ENV HF_HUB_OFFLINE=1