import os
import csv
from pathlib import Path
from datetime import datetime
from ultralytics import YOLO

# Images live here (matches your Task 1 structure)
IMAGES_DIR = Path("data/raw/images")
OUT_DIR = Path("data/processed/yolo")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "yolo_detections.csv"

# Use small model for laptops
MODEL_NAME = os.getenv("YOLO_MODEL", "yolov8n.pt")

# Confidence threshold
CONF_THRES = float(os.getenv("YOLO_CONF", "0.25"))


def infer_message_id(image_path: Path) -> int | None:
    """
    Your images are stored as: data/raw/images/{channel}/{message_id}.jpg
    Extract message_id from filename.
    """
    try:
        return int(image_path.stem)
    except Exception:
        return None


def classify_image(detected_labels: set[str]) -> str:
    """
    Simple categorization scheme:
    - promotional: person + container-like object (bottle/cup)
    - product_display: container-like object without person
    - lifestyle: person without product container
    - other: none
    NOTE: YOLO general labels vary; we use best-effort mapping.
    """
    has_person = "person" in detected_labels

    # YOLO COCO labels don't include "pill/cream", so we approximate containers.
    container_like = {"bottle", "cup", "bowl", "wine glass", "vase", "jar"}
    has_container = any(lbl in detected_labels for lbl in container_like)

    if has_person and has_container:
        return "promotional"
    if (not has_person) and has_container:
        return "product_display"
    if has_person and (not has_container):
        return "lifestyle"
    return "other"


def main():
    if not IMAGES_DIR.exists():
        print(f"No images folder found at {IMAGES_DIR}. Run Task 1 image download first.")
        return

    images = []
    for ext in ("*.jpg", "*.jpeg", "*.png", "*.webp"):
        images.extend(IMAGES_DIR.rglob(ext))

    if not images:
        print("No images found to analyze.")
        return

    print(f"Found {len(images)} images. Loading model: {MODEL_NAME}")
    model = YOLO(MODEL_NAME)

    # Write CSV header
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "run_ts",
            "channel_name",
            "message_id",
            "image_path",
            "detected_class",
            "confidence_score",
            "bbox_xyxy",
            "image_category"
        ])

        run_ts = datetime.utcnow().isoformat()

        for img_path in images:
            channel_name = img_path.parent.name
            message_id = infer_message_id(img_path)

            if message_id is None:
                continue

            # Run inference
            results = model.predict(
                source=str(img_path),
                conf=CONF_THRES,
                verbose=False
            )

            # results is a list, take first
            r0 = results[0]
            boxes = r0.boxes

            detected_labels_for_category = set()

            if boxes is None or len(boxes) == 0:
                # No detections
                image_category = classify_image(set())
                writer.writerow([run_ts, channel_name, message_id, str(img_path), None, None, None, image_category])
                continue

            # Each box: class id, conf, xyxy
            for b in boxes:
                cls_id = int(b.cls.item())
                conf = float(b.conf.item())
                label = model.names.get(cls_id, str(cls_id))
                xyxy = b.xyxy.squeeze().tolist()  # [x1,y1,x2,y2]
                bbox_str = ",".join([f"{x:.2f}" for x in xyxy])

                detected_labels_for_category.add(label)

                # Temporarily write row with placeholder category; update after loop? easiest: compute after
                # We'll store rows then write after category is known.
                # For simplicity, write per detection, category computed from full set after loop:
                # (We can compute category now based on detected_labels_for_category after adding label.)
                # But category should be same for all rows for this image; so compute after finishing.
            image_category = classify_image(detected_labels_for_category)

            # Write per detection rows now that category is known
            for b in boxes:
                cls_id = int(b.cls.item())
                conf = float(b.conf.item())
                label = model.names.get(cls_id, str(cls_id))
                xyxy = b.xyxy.squeeze().tolist()
                bbox_str = ",".join([f"{x:.2f}" for x in xyxy])

                writer.writerow([
                    run_ts,
                    channel_name,
                    message_id,
                    str(img_path),
                    label,
                    conf,
                    bbox_str,
                    image_category
                ])

    print(f"✅ YOLO detection done. Results saved to: {OUT_CSV}")


if __name__ == "__main__":
    main()
