# OtoCPA --- 15-Minute Live Demo Script

**Audience:** CPA firm owner/partner + potentially their team
**Pre-requisites:** OtoCPA installed with demo data loaded
**Browser:** Open to `http://127.0.0.1:8787/login`

---

## PRE-DEMO SETUP (Do this 10 minutes before the call)

### 1. Enable demo mode

Edit `otocpa.config.json` and set:

```json
{
  "demo_mode": true
}
```

### 2. Load demo data

```
python scripts/load_demo_data.py
```

This loads 50 pre-curated documents across 5 demo clients:
- **MARCEL** --- Marcel Tremblay, comptable independant
- **BOLDUC** --- Cabinet Bolduc CPA
- **DENTAIRE** --- Clinique Dentaire Saint-Laurent
- **BOUTIQUE** --- Boutique Mode Laval
- **TECHLAVAL** --- TechLaval Solutions Inc.

Each client has 10 documents including: normal invoices, fraud-flagged items, low-confidence AI extractions, meal receipts, bank statement matches, new vendor alerts, round number flags, insurance documents, and math mismatches.

### 3. Restart the dashboard

```
sc stop OtoCPA
sc start OtoCPA
```

Or manually:

```
python scripts/review_dashboard.py
```

### 4. Prepare your browser

- Open `http://127.0.0.1:8787/login`
- Log in as the owner account
- Set language to French (or English, depending on the audience)
- Have a second browser tab ready for the filing summary
- Have a sample receipt photo ready on your desktop (or use a demo document)

### 5. Close everything else

- Close all other browser tabs
- Close email and Slack
- Mute notifications
- Clear your desktop of clutter

---

## MINUTE 0:00--2:00 --- PROBLEM STATEMENT AND SETUP

### Narration:

> "Avant de commencer, une question rapide: combien d'heures par semaine votre equipe passe-t-elle a saisir des factures dans QuickBooks?"

*Wait for their answer. Write it down.*

> "OK, [X] heures. A 150 dollars de l'heure, c'est [math] par semaine de capacite non facturee. Par mois, ca fait [math]. C'est exactement ce que OtoCPA elimine."

> "Ce que je vais vous montrer en 15 minutes:"
>
> 1. "Comment un document est traite automatiquement"
> 2. "Comment la detection de fraude fonctionne"
> 3. "Comment le resume de production pre-remplit votre FPZ-500"
> 4. "Et si vous faites des missions d'audit, le module de dossiers de travail"
>
> "On commence."

### On screen:

Show the login page. Log in. The dashboard loads with the document queue visible.

---

## MINUTE 2:00--5:00 --- AUTOMATIC DOCUMENT EXTRACTION

### Narration:

> "Voici la file d'attente. Chaque ligne est un document --- une facture, un recu, un releve --- que le systeme a deja lu et classe."

**Action:** Click on a document with status "Ready" (green) for client MARCEL.

> "Regardez ce document. C'est une facture que le client Marcel a envoyee par courriel. Le systeme a automatiquement extrait:"
>
> - "Le fournisseur: [montrer le champ]"
> - "Le montant: [montrer]"
> - "La date: [montrer]"
> - "Le compte GL suggere: [montrer]"
> - "La TPS: [montrer le calcul] --- 5% sur le montant pre-taxe"
> - "La TVQ: [montrer le calcul] --- 9,975% sur le meme montant pre-taxe. Pas en cascade. En parallele. C'est la facon correcte au Quebec."

> "Votre equipe a juste a verifier et cliquer Approuver."

**Action:** Scroll down to show the confidence score.

> "Ici, le niveau de confiance: [X]%. Au-dessus de 85%, le systeme recommande l'approbation. En dessous, il le met en revision obligatoire. Votre equipe decide toujours."

**Action:** Click "Approuver" on the document.

> "Un clic. C'est fait. Le document est pret pour la soumission a QuickBooks."

### Key talking point:

> "Tout ce processus --- de la photo a l'approbation --- prend 10 secondes. Comparez ca a 5 minutes de saisie manuelle par document."

---

## MINUTE 5:00--8:00 --- FRAUD DETECTION

### Narration:

> "Maintenant, je vais vous montrer quelque chose qu'aucun autre logiciel ne fait."

**Action:** Go back to the queue. Filter by status "NeedsReview" (or find a document with fraud flags). Look for the BOLDUC client's fraud-flagged document.

> "Ce document a ete automatiquement mis en revision. Regardons pourquoi."

**Action:** Click on the flagged document. Scroll to the fraud detection section.

> "Voici les alertes de fraude. Le systeme a detecte:"
>
> [Read whatever flags are showing. Examples:]
> - "'Doublon exact' --- meme montant, meme fournisseur, dans les 30 derniers jours. Severite: HAUTE."
> - "'Transaction de fin de semaine' --- facture datee un samedi, montant superieur a 200 dollars."
> - "'Anomalie de montant' --- ce montant est a plus de 2 ecarts-types de la moyenne pour ce fournisseur."

> "Le systeme ne bloque pas la facture --- il la signale. Votre equipe voit l'alerte, investigue, et decide. Si c'est legitime, on approuve avec une raison documentee. Si c'est suspect, on met en attente."

**Action:** Show the "fraud override" option --- the fact that it requires a manager/owner role and a written reason.

> "Pour passer outre une alerte de fraude, il faut un role de gestionnaire ou proprietaire, et il faut ecrire la raison. Cette raison est enregistree dans la piste d'audit. Impossible de la modifier apres coup."

### Key talking point:

> "13 regles de detection de fraude. Toutes basees sur des regles, pas sur l'IA. Toutes connectees au pipeline d'approbation. Si une fraude est detectee avec severite CRITIQUE ou HAUTE, le document ne peut pas etre auto-approuve. Point final."

---

## MINUTE 8:00--11:00 --- FILING SUMMARY AND REVENU QUEBEC

### Narration:

> "Passons a ce qui vous fait gagner le plus de temps pendant la production."

**Action:** Navigate to `/filing_summary` in the browser.

> "Voici le resume de production. Toutes les transactions approuvees pour [client] pour la periode [mois]."

**Action:** Show the summary page with the GST/QST lines.

> "Regardez les lignes:"
>
> - "Ligne 103 --- Ventes et fournitures taxables: [montant]"
> - "Ligne 106 --- TPS percue: [montant]"
> - "Ligne 108 --- Total TPS percue: [montant]"
> - "Ligne 205 --- TVQ percue: [montant]"
> - "Ligne 207 --- CTI reclames: [montant]"
> - "Ligne 209 --- RTI reclames: [montant]"

> "Tout ca est calcule automatiquement a partir des transactions approuvees. Y compris la recuperation partielle pour les repas (50%), l'assurance (9% non recuperable), et les exemptions."

**Action:** Click "Telecharger PDF" or navigate to `/revenu_quebec/pdf`.

> "Et ici, le PDF pre-rempli pour Revenu Quebec. Votre equipe n'a qu'a reporter les chiffres dans la declaration. Plus de calculs manuels, plus d'erreurs."

### If they use Quick Method:

**Action:** Navigate to `/revenu_quebec/set_config` and show the Quick Method toggle.

> "Et si votre client est sur la methode rapide, on configure ca ici. Le calcul change automatiquement. Client par client."

### Key talking point:

> "Ce resume, a la main, ca prend combien de temps? 45 minutes? Une heure? Avec OtoCPA, c'est instantane. Et c'est toujours juste, parce que c'est du math, pas une estimation."

---

## MINUTE 11:00--14:00 --- AUDIT MODULE

### Narration:

> [First, check if they do assurance work]
> "Est-ce que votre cabinet fait des missions de verification, d'examen ou de compilation?"

**If yes:**

> "Parfait. Alors regardez ca."

**Action:** Navigate to `/engagements`.

> "Voici la liste des engagements. On a un engagement d'audit en cours pour [client]."

**Action:** Click on an engagement to show details.

> "Chaque engagement suit les normes CAS. Regardez:"

**Action:** Navigate to `/audit/materiality`.

> "CAS 320 --- Importance relative. Le seuil de signification est calcule automatiquement. Signification de planification, d'execution, et seuil de trivialite."

**Action:** Navigate to `/working_papers`.

> "Dossiers de travail generes automatiquement depuis le plan comptable. Chaque compte marque 'significatif' a un badge et requiert des procedures documentees."

**Action:** Show the assertion matrix.

> "Matrice d'assertions CAS 500: exhaustivite, exactitude, existence, cesure, classification. Votre equipe coche au fur et a mesure. Si un element significatif n'a pas d'assertion testee, le systeme bloque l'emission du rapport."

**Action:** Navigate to `/financial_statements`.

> "Et les etats financiers --- bilan et etat des resultats --- generes depuis la balance de verification. Telechargeables en PDF."

**If they don't do assurance work:**

> "Pas de probleme. Le module d'audit est dans le forfait Entreprise. Pour l'instant, concentrons-nous sur ce qui vous fait gagner du temps au quotidien. Si un jour vous ajoutez des missions de certification, la fonctionnalite sera la."

*Skip to minute 14.*

---

## MINUTE 14:00--15:00 --- CALL TO ACTION

### Narration:

> "Voila ce que OtoCPA fait en 15 minutes de demo. Imaginez ce que ca fait sur un mois entier de production."

*Pause. Let it sink in.*

> "Pour [NomDuCabinet] avec [X] clients, on estime [Y] heures economisees par mois. A 150 dollars de l'heure, c'est [Z] dollars de capacite recuperee. OtoCPA [forfait] coute [prix] par mois."

*Pause again.*

> "Voici ce que je propose: on fait un pilote de 30 jours. Gratuit. Je configure 5 de vos clients cette semaine. Vous utilisez le logiciel normalement avec votre equipe. Apres 30 jours, vous mesurez les heures economisees et vous decidez."

> "Aucune obligation. Aucun paiement pendant le pilote. Si ca ne vous convient pas, on desinstalle et on reste amis."

> "Est-ce qu'on commence?"

*Stop talking. Wait for their response.*

### If they say yes:

> "Parfait. Est-ce que [jour] a [heure] fonctionne pour l'installation? Ca prend 30 minutes a distance. J'ai besoin d'un acces a votre serveur et de la liste de vos 5 premiers clients."

### If they say they need to think:

> "Bien sur. Est-ce que je peux vous envoyer un resume par courriel et vous recontacter [jour precis]?"

### If they say no:

> "Je comprends. Est-ce que je peux vous laisser notre fiche-resume? Si la situation change, vous aurez mes coordonnees."

---

## POST-DEMO CHECKLIST

- [ ] Send follow-up email within 2 hours (use template in `EMAIL_TEMPLATES.md`)
- [ ] Include the specific numbers from the demo (clients, hours, ROI)
- [ ] If they said yes to pilot: schedule installation within 48 hours
- [ ] If they said "think about it": set calendar reminder for follow-up date
- [ ] If they said no: add to quarterly touch list
- [ ] Log the demo in your CRM with notes on their specific pain points
- [ ] Disable demo mode after the demo if this is on a client's machine:

```json
{
  "demo_mode": false
}
```

---

## DEMO RECOVERY SCENARIOS

### If the dashboard doesn't load:

> "Un instant, le serveur redemarre." Run `python scripts/autofix.py --lang en` in a terminal. While it runs: "C'est justement l'outil de diagnostic automatique en action. Il verifie 14 points de sante et repare ce qui peut l'etre automatiquement."

### If a document shows extraction errors:

> "Parfait --- c'est exactement ce que le systeme devrait faire. Quand la confiance est basse, le document est envoye en revision. Votre equipe corrige, et le systeme apprend de la correction pour la prochaine fois."

### If the demo data looks sparse:

> "C'est un environnement de demonstration avec des donnees synthetiques. Dans votre vrai environnement, chaque document de vos clients apparaitrait ici avec les vrais montants et fournisseurs."

### If they ask about QuickBooks integration:

> "L'integration QuickBooks Online est directe --- on soumet les ecritures approuvees via l'API. Pour la demo, on ne se connecte pas a un vrai compte QBO, mais je peux vous montrer a quoi ressemble une ecriture construite."

Navigate to a posted document and show the posting job details.

### If they ask about a feature you don't know:

> "Excellente question. Je vais verifier et vous revenir la-dessus par courriel aujourd'hui."

Never bluff. Never guess. "Je verifie et je vous reviens" is always a valid answer.
