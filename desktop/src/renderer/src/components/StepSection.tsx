import { motion } from "framer-motion";

export function StepSection({
  body,
  children,
}: {
  body: string;
  children: React.ReactNode;
}) {
  return (
    <motion.div
      className="step-section clean-step-section"
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.22 }}
    >
      <div className="section-heading clean-heading">
        <p>{body}</p>
      </div>
      {children}
    </motion.div>
  );
}
