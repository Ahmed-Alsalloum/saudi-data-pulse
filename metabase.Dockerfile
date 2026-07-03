# The official Metabase image is Alpine-based, and DuckDB's native library is
# glibc-only — it segfaults under musl even with gcompat. Run the Metabase jar
# on a Debian-based JRE instead.
FROM eclipse-temurin:21-jre
ADD https://downloads.metabase.com/latest/metabase.jar /app/metabase.jar
ENV MB_PLUGINS_DIR=/plugins
RUN mkdir -p /plugins /metabase-data
EXPOSE 3000
CMD ["java", "-jar", "/app/metabase.jar"]
