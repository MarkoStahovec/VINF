FROM coady/pylucene

WORKDIR /usr/src/app

# Copy the rest of your project
# COPY . .

RUN pip install requests
RUN pip install tqdm
