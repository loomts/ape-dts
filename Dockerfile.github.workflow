FROM alpine:3.18

ARG MODULE_NAME="dt-main"
ARG TARGETOS 
ARG TARGETARCH

COPY --chmod=777 ${TARGETARCH}-unknown-${TARGETOS}-gnu-${MODULE_NAME} /ape-dts
COPY log4rs.yaml /log4rs.yaml

ENTRYPOINT [ "/ape-dts" ]
