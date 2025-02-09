# Copyright ⓒ 2024-2025 Peter Morgan <peter.james.morgan@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

FROM --platform=$BUILDPLATFORM tonistiigi/xx AS xx

FROM --platform=$BUILDPLATFORM rust:alpine AS builder
COPY --from=xx / /
RUN apk add clang lld
RUN rustup target add $(xx-cargo --print-target-triple)

ARG PACKAGE=tansu-server
WORKDIR /usr/src
ADD / /usr/src/

ARG TARGETPLATFORM
RUN xx-apk add --no-cache musl-dev zlib-dev
RUN xx-cargo build --package ${PACKAGE} --release --target-dir ./build
RUN xx-verify --static ./build/$(xx-cargo --print-target-triple)/release/${PACKAGE}

RUN <<EOF
mkdir -p /image/schema /image/tmp /image/etc/ssl
cp -v build/$(xx-cargo --print-target-triple)/release/${PACKAGE} /image
cp -v LICENSE /image
cp -rv /etc/ssl /image/etc
EOF

FROM scratch
COPY --from=builder /image /
ENV TMP=/tmp
ENTRYPOINT [ "/tansu-server" ]
