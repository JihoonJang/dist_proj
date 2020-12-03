let a = {a: 1, b: 2, c: 3, e: 4, d: 5};
console.log(Object.entries(a));
console.log(Object.entries(a).sort((a, b) => b[1] - a[1]));