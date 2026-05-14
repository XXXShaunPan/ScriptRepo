const nav = document.querySelector("[data-nav]");
const scene = document.querySelector("[data-scene]");
const revealItems = document.querySelectorAll(".reveal");

function updateNav() {
  nav.classList.toggle("scrolled", window.scrollY > 24);
}

function updateScene(event) {
  if (!scene) {
    return;
  }
  const x = (event.clientX / window.innerWidth - 0.5) * 18;
  const y = (event.clientY / window.innerHeight - 0.5) * 12;
  scene.style.setProperty("--scene-x", `${x}px`);
  scene.style.setProperty("--scene-y", `${y}px`);
}

const observer = new IntersectionObserver(
  (entries) => {
    for (const entry of entries) {
      if (entry.isIntersecting) {
        entry.target.classList.add("visible");
        observer.unobserve(entry.target);
      }
    }
  },
  { threshold: 0.16 },
);

for (const item of revealItems) {
  observer.observe(item);
}

window.addEventListener("scroll", updateNav, { passive: true });
window.addEventListener("pointermove", updateScene, { passive: true });
updateNav();
