# Use Mozilla's sbt Docker image as the base image
FROM mozilla/sbt:latest as build

WORKDIR /app
RUN apt install git
# Clone the git repo
RUN git clone https://github.com/ronith256/bigdataproj.git /app

# Compile the project using sbt
RUN sbt compile

# Final stage: Create a container for running the application
FROM openjdk:8-jre

WORKDIR /app

# Copy compiled jar file from the build stage
COPY --from=build /app/target/scala-*/app-assembly-*.jar /app/app.jar

# Define the default command to run the app
CMD ["java", "-jar", "app.jar"]

# Expose port 3000 for the Grafana Dashboard or the sbt-app if it serves HTTP requests
# EXPOSE 3001