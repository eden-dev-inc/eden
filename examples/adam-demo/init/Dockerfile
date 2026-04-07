FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Vertical framework
COPY verticals/ ./verticals/
COPY schemas/ ./schemas/

# Entrypoint (dispatches to vertical)
COPY load_data.py .

# Retail-specific (original scripts, kept for backward compat)
COPY load_data_retail.py .
COPY generate_ecommerce_data.py .

# Data is mounted as a volume from docker-compose, not baked into the image

CMD ["sh", "-c", "python -u load_data.py && if [ \"$VERTICAL\" = 'retail' ] || [ \"$VERTICAL\" = 'stonebreaker' ] || [ -z \"$VERTICAL\" ]; then python -u generate_ecommerce_data.py --scale ${SCALE:-demo}; fi"]
