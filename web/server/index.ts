const port = Number(process.env.PORT ?? 3000);

console.log(`spark-ui-service web dev server placeholder listening on ${port}`);

// Keep the placeholder process alive for `tsx watch` during local development.
setInterval(() => {}, 1 << 30);
