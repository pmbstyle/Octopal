export function ReviewItem({ label, value }: { label: string; value: string }) {
  return (
    <div className="review-item">
      <small>{label}</small>
      <strong>{value}</strong>
    </div>
  );
}
