package sjdb;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class Estimator implements PlanVisitor {


	public Estimator() {
		// empty constructor
	}

	/* 
	 * Create output relation on Scan operator
	 *
	 * Example implementation of visit method for Scan operators.
	 */
	public void visit(Scan op) {
		Relation input = op.getRelation();
		Relation output = new Relation(input.getTupleCount());
		
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			output.addAttribute(new Attribute(iter.next()));
		}
		
		op.setOutput(output);
	}

	public void visit(Project op) {
		Relation input = op.getInput().getOutput();
		Relation output = new Relation(input.getTupleCount());
		Iterator<Attribute> iter = input.getAttributes().iterator();
		while (iter.hasNext()) {
			Attribute addElement = iter.next();
			Iterator<Attribute> iterOp = op.getAttributes().iterator();
			while (iterOp.hasNext()) {
				if (iterOp.next().equals(addElement)) {
					output.addAttribute(new Attribute(addElement));
				}
			}
		}

		op.setOutput(output);
	}

	public void visit(Select op) {
		Relation input = op.getInput().getOutput();
		Relation output;
		int T = input.getTupleCount();
		int V = 0;
		if (op.getPredicate().equalsValue()) {
			Attribute attr = op.getPredicate().getLeftAttribute();
			attr = input.getAttribute(attr);
			T = (int) Math.ceil((double)T / attr.getValueCount());
			V = 1;
			output = new Relation(T);
			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute addElement = iter.next();
				if (addElement.equals(attr)) {
					output.addAttribute(new Attribute(attr.getName(), V));
				}else {
					output.addAttribute(new Attribute(addElement));
				}
			}
		}else {
			Attribute attrL = op.getPredicate().getLeftAttribute();
			Attribute attrR = op.getPredicate().getRightAttribute();
			attrL = input.getAttribute(attrL);
			attrR = input.getAttribute(attrR);
			T = (int) Math.ceil((double) T / Math.max(attrL.getValueCount(), attrR.getValueCount()));
			V = Math.min(attrL.getValueCount(), attrR.getValueCount());
			output = new Relation(T);
			Iterator<Attribute> iter = input.getAttributes().iterator();
			while (iter.hasNext()) {
				Attribute addElement = iter.next();
				if (addElement.equals(attrL)) {
					output.addAttribute(new Attribute(attrL.getName(), V));
				}else if (addElement.equals(attrR)) {
					output.addAttribute(new Attribute(attrR.getName(), V));
				}else {
					output.addAttribute(new Attribute(addElement));
				}
			}
		}
		op.setOutput(output);

	}

	public void visit(Product op) {
		Relation inputL = op.getLeft().getOutput();
		Relation inputR = op.getRight().getOutput();
		Relation output = new Relation(inputL.getTupleCount() * inputR.getTupleCount());
		Iterator<Attribute> iterL = inputL.getAttributes().iterator();
		Iterator<Attribute> iterR = inputR.getAttributes().iterator();
		while (iterL.hasNext()) {
			output.addAttribute(new Attribute(iterL.next()));
		}
		while (iterR.hasNext()) {
			output.addAttribute(new Attribute(iterR.next()));
		}
		op.setOutput(output);

	}
	
	public void visit(Join op) {
		if (op.getPredicate().equalsValue()) {
			throw new RuntimeException("need two attr");
		}else {
			Relation inputL = op.getLeft().getOutput();
			Relation inputR = op.getRight().getOutput();
			Attribute attrL = op.getPredicate().getLeftAttribute();
			Attribute attrR = op.getPredicate().getRightAttribute();
			attrL = inputL.getAttribute(attrL);
			attrR = inputR.getAttribute(attrR);
			int T = (int) Math.ceil((double) inputL.getTupleCount() * inputR.getTupleCount() /
					Math.max(attrL.getValueCount(), attrR.getValueCount()));
			int V = Math.min(attrL.getValueCount(), attrR.getValueCount());
			Relation output = new Relation(T);
			Iterator<Attribute> iterL = inputL.getAttributes().iterator();
			Iterator<Attribute> iterR = inputR.getAttributes().iterator();
			while (iterL.hasNext()) {
				Attribute addElement = iterL.next();
				if (addElement.equals(attrL)) {
					output.addAttribute(new Attribute(attrL.getName(), V));
				} else {
					output.addAttribute(new Attribute(addElement));
				}
			}
			while (iterR.hasNext()) {
				Attribute addElement = iterR.next();
				if (addElement.equals(attrR)) {
					output.addAttribute(new Attribute(attrR.getName(), V));
				} else {
					output.addAttribute(new Attribute(addElement));
				}
			}
			op.setOutput(output);
		}
	}
}
