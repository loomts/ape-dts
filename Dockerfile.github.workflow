FROM gcr.io/distroless/cc:debug

ARG MODULE_NAME="dt-main"
ARG TARGETOS 
ARG TARGETARCH

COPY --chmod=777 ${TARGETARCH}-unknown-${TARGETOS}-gnu-${MODULE_NAME} /ape-dts
COPY log4rs.yaml /log4rs.yaml

ENTRYPOINT [ "/ape-dts" ]
