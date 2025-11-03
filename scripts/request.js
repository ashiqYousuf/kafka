const url = "http://127.0.0.1:8000/users";

for (let i = 1; i <= 1000; i++) {
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: `User${i}`,
      password: "test123@",
      address: `addr${i}`
    })
  })
  .then(res => res.text())
  .then(data => console.log(`✅ Request ${i} done:`, data))
  .catch(err => console.error(`❌ Request ${i} failed:`, err));
}
