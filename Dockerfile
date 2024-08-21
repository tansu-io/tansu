# Copyright ⓒ 2024 Peter Morgan <peter.james.morgan@gmail.com>
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

ARG BUILD_IMAGE=rust:1.80

FROM ${BUILD_IMAGE} AS builder

ARG PACKAGE=tansu-server

WORKDIR /usr/src
ADD / /usr/src/
RUN cargo build --package ${PACKAGE} --release

RUN <<EOF
mkdir /image /data

# copy any dynamically linked libaries used
for lib in $(ldd target/release/* 2>/dev/null|grep "=>"|awk '{print $3}'|sort|uniq); do
    mkdir -p $(dirname /image$lib)
    cp -Lv $lib /image$lib
done

# ensure that the link loader is present
case `arch` in
    x86_64)
        mkdir -p /image/lib64
        cp -v /lib64/ld-linux*.so.* /image/lib64;;

    aarch64)
        cp -v /lib/ld-linux*.so.* /image/lib;;
esac

# copy the executable
cp -v target/release/${PACKAGE} /image
EOF

FROM scratch
COPY --from=builder /image /data /
ENTRYPOINT [ "/tansu-server" ]
