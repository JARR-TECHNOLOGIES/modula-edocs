FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    nginx \
    supervisor \
    libnginx-mod-http-modsecurity \
    libcairo2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libpangoft2-1.0-0 \
    libgdk-pixbuf-2.0-0 \
    libffi-dev \
    libglib2.0-0 \
    libgirepository1.0-dev \
    gir1.2-pango-1.0 \
    gir1.2-gobject-2.0 \
    fonts-dejavu-core \
    curl \
    && apt-get clean


# ModSecurity OWASP CRS setup
RUN mkdir -p /etc/nginx/modsecurity/owasp-crs \
    && curl -L https://github.com/coreruleset/coreruleset/archive/refs/tags/v4.3.0.tar.gz \
        -o /tmp/crs.tar.gz \
    && tar -xzf /tmp/crs.tar.gz -C /tmp \
    && cp -R /tmp/coreruleset-4.3.0/crs-setup.conf.example /etc/nginx/modsecurity/crs-setup.conf \
    && cp -R /tmp/coreruleset-4.3.0/rules/* /etc/nginx/modsecurity/owasp-crs/ \
    && rm -rf /tmp/crs.tar.gz /tmp/coreruleset-4.3.0

# WORKDIR
WORKDIR /app
COPY api/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY api/ /app/api/

# Create non-root user
RUN addgroup --system modula && adduser --system --ingroup modula --home /app modula
RUN chown -R modula:modula /app

# NGINX
COPY build/nginx.conf /etc/nginx/nginx.conf
COPY build/modsecurity.conf /etc/nginx/modsecurity/modsecurity.conf
COPY build/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY build/*.sh /app/infra/
RUN chmod +x /app/infra/*.sh

EXPOSE 8080
CMD ["supervisord", "-n"]
