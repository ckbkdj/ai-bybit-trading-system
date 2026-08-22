const zlib = require("pako")
const ee = require("crypto-js")

// function btoa(str) {
//   return Buffer.from(str, 'utf-8').toString('base64');
// }
var getE = function (t,pw) {
    // header_user = "QzwFvyFANPusLHNr5pejdVicai7D+9/6OG/upcio3wR/UDUoiTOfkPwwxCNuSzik"
    header_user = t
    e = "/api/openInterest/v3/chart"
    // n = btoa("coinglass".concat(e, "coinglass"))v1
    n = btoa(pw)
    n = n.substring(0, 16)
    n = Yt(header_user, n);
    return n;
};


var Yt = function (t, e) {
    qt = ee;
    var n = function (t) {
        var e, n = zlib.inflate(
          new Uint8Array(
            t.match(/[\da-f]{2}/gi).map(function (t) {
              return parseInt(t, 16);
            })
          )
        ), r = "", i = 16384;
        for (e = 0; e < n.length / i; e++)
            r += String.fromCharCode.apply(null, n.slice(e * i, (e + 1) * i));
        return r += String.fromCharCode.apply(null, n.slice(e * i)),
            decodeURIComponent(escape(r))
    }(qt.AES.decrypt(t, qt.enc.Utf8.parse(e), {
        mode: qt.mode.ECB,
        padding: qt.pad.Pkcs7
    }).toString(qt.enc.Hex));
    return '"' == n.charAt(0) && (n = n.substring(1, n.length)),
    '"' == n.charAt(n.length - 1) && (n = n.substring(0, n.length - 1)),
        n
};