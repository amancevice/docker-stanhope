FROM amancevice/pandas:0.20.2-python3

# Install dependencies & add stanhope user
RUN apt-get update && \
    apt-get install -y mdbtools && \
    pip install \
        IPython==5.4.1 \
        click==6.7.0 \
        ardec==0.0.3 && \
    useradd -b /home -U -m stanhope

# Set up app ENV
VOLUME /data
WORKDIR /data

# Install app
COPY stanhope /stanhope
RUN pip install -e /stanhope
ENTRYPOINT ["stanhope"]

# Run as stanhope user
USER stanhope
