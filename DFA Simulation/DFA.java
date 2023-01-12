package fa.dfa;

import fa.State;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;

/**
 * September 24, 2022
 * The class implements DFAInterface and creates a formal definition of a given
 * DFA. 
 * 
 * @author cpinney
 *
 */

public class DFA implements DFAInterface {

	private LinkedHashSet<DFAState> finalStates;
	private DFAState startState;
	private LinkedHashSet<DFAState> otherStates;
	private ArrayList<Character> delta;
	private HashSet<Character> sigma;
	private ArrayList<String> lang;
	
	
	public DFA() {

        this.finalStates = new LinkedHashSet<DFAState>();
        this.startState = new DFAState("");
        this.otherStates = new LinkedHashSet<DFAState>();
        this.delta = new ArrayList<Character>();
        this.sigma = new HashSet<Character>();
        this.lang = new ArrayList<String>();

    }

    /**
     * Construct the textual representation of the DFA, for example
     * A simple two state DFA
     * Q = { a b }
     * Sigma = { 0 1 }
     * delta =
     * 0 1
     * a a b
     * b a b
     * q0 = a
     * F = { b }
     * 
     * The order of the states and the alphabet is the order
     * in which they were instantiated in the DFA.
     * 
     * @return String representation of the DFA
     */
    @Override
    public String toString() {

        // main string builder
    	StringBuilder sb = new StringBuilder();
    	// string builder for Q = { set of states }
        StringBuilder sbQ = new StringBuilder();
        // string builder for Sigma = { alphabet of DFA }
        StringBuilder sbSigma1 = new StringBuilder();
        // alternate format for delta table
        StringBuilder sbSigma2 = new StringBuilder();
        // string builder for F = { final states }
        StringBuilder sbF = new StringBuilder();
        // string builder for delta = { delta table }
        StringBuilder sbDelta = new StringBuilder();

        // add final states to Q
        for (DFAState ds : finalStates) {
        	sbQ.append(ds + " ");
        }
        // add start state to Q
        sbQ.append(startState + " ");
        
        // add other states to Q
        for (DFAState ds : otherStates) {
        	sbQ.append(ds + " ");
        }
        // add alphabet characters to sigma
        for (char c : sigma) {
        	sbSigma1.append(c);
        	sbSigma1.append(" ");
        }
        // alternate sigma
        for (char c : sigma) {
        	sbSigma2.append(c);
        	sbSigma2.append("\t");
        }
        // add final states to F
        for (DFAState ds : finalStates) {
        	sbF.append(ds + " ");
        }
        // construct delta table
        int i = 0;
        while (i < delta.size()) {
        	
        	sbDelta.append(delta.get(i) + "\t" + delta.get(i+1) + "\t" + delta.get(i+3) + "\n\t");
        	i += 4;
        }
        // get rid of extra newline
        sbDelta.deleteCharAt(sbDelta.length()-2);
        
        // add all string builders to main string builder
        sb.append("Q = { " + sbQ + "}\n");
        sb.append("Sigma = { " + sbSigma1 + "}\n");
        sb.append("delta = \n\t\t" + sbSigma2 + "\n\t" +  sbDelta + "\n");
        sb.append("q0 = " + startState.toString() + "\n");
        sb.append("F = { " + sbF + "}\n");

        return sb.toString();
    }

    /**
     * Simulates a DFA on input s to determine
     * whether the DFA accepts s.
     * 
     * @param s - the input string
     * @return true if s in the language of the DFA and false otherwise
     */
    @Override
    public boolean accepts(String s) {

    	// start trace at initial state
    	DFAState thisState = startState;
        
        // to trace the whole string 
    	int i = 0;
        while (i < s.length()) {
        	
        	// get next state
        	thisState = getToState(thisState, s.charAt(i));
        	i++;
        	
        }
        
        // create arrayList of names of final states
        ArrayList<String> arr = new ArrayList<String>();
        for (DFAState ds : finalStates) {
        	arr.add(ds.getName());
        }
        
        // if thisState's name is in the array of final states, we can accept
        // this string as a part of the DFA's language
        if (arr.contains(thisState.getName())) {
        	
        	return true;
        // otherwise, we reject this string	
        } else {
        	
        	return false;
        }
    }

    /**
     * Uses transition function delta of FA
     * 
     * @param from   the source state
     * @param onSymb the label of the transition
     * @return the sink state.
     */
    @Override
    public DFAState getToState(DFAState from, char onSymb) {

        DFAState thisState = from;
        
        // create string to compare with transition strings
        String str = from.getName() + String.valueOf(onSymb);
        
        // iterate through each transition string in lang
        int i = 0;
        while (i < lang.size()-2) {
        	
        	for (String s : lang) {
        		
        		// if str is the same as the first to elements of 
        		if (str.equals(s.substring(0, 2))) {
        			
        			DFAState newState = new DFAState(String.valueOf(s.charAt(2)));
        			thisState = newState;
        			
        		} 
        		
        	}
        	
        	i++;
        	
        }
        
        return thisState;
    }

    /**
     * Computes a copy of this DFA
     * which language is the complement
     * of this DFA's language.
     * 
     * @return a copy of this DFA
     */
    @Override
    public DFA complement() {

        DFA thisDFA = new DFA();
        
        thisDFA.startState = startState;
        if (otherStates.isEmpty()) {
        	otherStates.add(startState);
        }
        LinkedHashSet<DFAState> temp = finalStates;
        thisDFA.finalStates = otherStates;
        thisDFA.otherStates = temp;
        thisDFA.sigma = sigma;
        thisDFA.lang = lang;
        
        return thisDFA;
        
    }

    /**
     * Adds the initial state to the DFA instance
     * 
     * @param name is the label of the start state
     */
    public void addStartState(String name) {

        DFAState thisState = new DFAState(name);
        
        startState = thisState;
    	
    }

    /**
     * Adds a non-final, not initial state to the DFA instance
     * 
     * @param name is the label of the state
     */
    public void addState(String name) {

        DFAState thisState = new DFAState(name);
    	
    	otherStates.add(thisState);
    	
    }

    /**
     * Adds a final state to the DFA
     * 
     * @param name is the label of the state
     */
    public void addFinalState(String name) {

    	DFAState thisState = new DFAState(name);
    	
    	finalStates.add(thisState);
    	
    }

    /**
     * Adds the transition to the DFA's delta data structure
     * 
     * @param fromState is the label of the state where the transition starts
     * @param onSymb    is the symbol from the DFA's alphabet.
     * @param toState   is the label of the state where the transition ends
     */
    public void addTransition(String fromState, char onSymb,
            String toState) {
    	
    	String result = fromState + toState;
    	char[] arr = result.toCharArray();
    	
    	for (char c : arr) {
    		
    		delta.add(c);
    	}
    	
    	result = fromState + onSymb + toState;
    	lang.add(result);
    	
    	sigma.add(onSymb);

    }

    /**
     * Getter for Q
     * 
     * @return a set of states that FA has
     */
    public Set<DFAState> getStates() {

        HashSet<DFAState> thisSet = new HashSet<DFAState>();

        thisSet.add(startState);
        
        for (DFAState ds : otherStates) {
        	
        	thisSet.add(ds);
        }

        for (DFAState ds : otherStates) {
        	
        	thisSet.add(ds);
        }

        return thisSet;
    }

    /**
     * Getter for F
     * 
     * @return a set of final states that FA has
     */
    public Set<? extends State> getFinalStates() {

        return finalStates;
    }

    /**
     * Getter for q0
     * 
     * @return the start state of FA
     */
    public DFAState getStartState() {
        
    	return startState;
    }

    /**
     * Getter for Sigma
     * 
     * @return the alphabet of FA
     */
    public Set<Character> getABC() {

        return sigma;
    }

}
