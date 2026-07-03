# The official Metabase image is Alpine-based (musl libc); the DuckDB driver's
# native library needs glibc's libstdc++. gcompat + libstdc++ bridge the gap.
FROM metabase/metabase:latest
RUN apk add --no-cache libstdc++ gcompat
