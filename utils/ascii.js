/**
 * Created by linshiding on 3/10/15.
 */
module.exports = {
  'NUL': 0,
  'SOH': 1,
  'STX': 2,
  'ETX': 3,
  'EOT': 4,
  'ENQ': 5,
  'ACK': 6,
  'BEL': 7,
  'BS': 8,
  'TAB': 9,
  '\t': 9,
  'LF': 10,
  '\n': 10,
  'VT': 11,
  'FF': 12,
  'CR': 13,
  '\r': 13,
  'SO': 14,
  'SI': 15,
  'DLE': 16,
  'DC1': 17,
  'DC2': 18,
  'DC3': 19,
  'DC4': 20,
  'NAK': 21,
  'SYN': 22,
  'ETB': 23,
  'CAN': 24,
  'EM': 25,
  'SUB': 26,
  'ESC': 27,
  'FS': 28,
  'GS': 29,
  'RS': 30,
  'US': 31,
  ' ': 32,
  'SPACE': 32,
  '!': 33,
  '"': 34,
  '#': 35,
  '$': 36,
  '%': 37,
  '&': 38,
  '\'': 39,
  '(': 40,
  ')': 41,
  '*': 42,
  '+': 43,
  ',': 44,
  '-': 45,
  '.': 46,
  '/': 47,
  '0': 48,
  '1': 49,
  '2': 50,
  '3': 51,
  '4': 52,
  '5': 53,
  '6': 54,
  '7': 55,
  '8': 56,
  '9': 57,
  ':': 58,
  ';': 59,
  '<': 60,
  '=': 61,
  '>': 62,
  '?': 63,
  '@': 64,
  'A': 65,
  'B': 66,
  'C': 67,
  'D': 68,
  'E': 69,
  'F': 70,
  'G': 71,
  'H': 72,
  'I': 73,
  'J': 74,
  'K': 75,
  'L': 76,
  'M': 77,
  'N': 78,
  'O': 79,
  'P': 80,
  'Q': 81,
  'R': 82,
  'S': 83,
  'T': 84,
  'U': 85,
  'V': 86,
  'W': 87,
  'X': 88,
  'Y': 89,
  'Z': 90,
  '[': 91,
  '\\': 92,
  ']': 93,
  '^': 94,
  '_': 95,
  '`': 96,
  'a': 97,
  'b': 98,
  'c': 99,
  'd': 100,
  'e': 101,
  'f': 102,
  'g': 103,
  'h': 104,
  'i': 105,
  'j': 106,
  'k': 107,
  'l': 108,
  'm': 109,
  'n': 110,
  'o': 111,
  'p': 112,
  'q': 113,
  'r': 114,
  's': 115,
  't': 116,
  'u': 117,
  'v': 118,
  'w': 119,
  'x': 120,
  'y': 121,
  'z': 122,
  '{': 123,
  '|': 124,
  '}': 125,
  '~': 126,

  isNumber: function(str) {
    let buf = Buffer.from(str);
    for (let i=0; i<buf.length; i++) {
      if ((buf[i]<48 || buf[i]>57) && buf[i]!=46)
        return false;
    }
    return true;
  },

  isString: function(str) {
    let len = str.length;
    let begin = str.charCodeAt(0);
    return (begin == 34 || begin == 39) && begin == str.charCodeAt(len - 1);
  },

  isAlphaDigit: function (str) {
    let buf = Buffer.from(str);
    for (let i=0; i<buf.length; i++) {
      if (!(buf[i]>=48 && buf[i]<=57
        || buf[i]>=97 && buf[i]<=122
        || buf[i]>=65 && buf[i]<=90))
        return false;
    }
    return true;
  },

  isIdentifier: function (str) {
    let buf = Buffer.from(str);
    for (let i=0; i<buf.length; i++) {
      if (!(buf[i]>=48 && buf[i]<=57
        || buf[i]>=97 && buf[i]<=122
        || buf[i]>=65 && buf[i]<=90
        || buf[i]==95))
        return false;
    }
    return true;
  }
};
