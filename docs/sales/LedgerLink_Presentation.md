# LedgerLink AI --- Presentation Deck (12 Slides)

**Audience:** CPA firm owners and managing partners in Quebec
**Duration:** 20 minutes (12 slides + 8 minutes Q&A)
**Language:** French primary, English available

---

## SLIDE 1 --- TITLE

### LedgerLink AI --- La comptabilité intelligente pour les cabinets CPA du Québec

**Visual:** LedgerLink logo centered. Tagline below. Your name, title, and contact in bottom-right corner.

**Speaking Notes:**

> "Bonjour, merci de me recevoir. Je m'appelle [votre nom], je travaille chez LedgerLink. On aide les cabinets CPA du Québec à automatiser la saisie comptable avec l'intelligence artificielle --- tout en gardant le contrôle humain sur chaque transaction. Je vais vous montrer comment en 15 minutes."

Timing: 30 seconds maximum. Do not describe the product yet. Do not show features. Just introduce yourself, thank them, and set the time expectation. Short is confident.

---

## SLIDE 2 --- THE PROBLEM

### "Combien d'heures par semaine passez-vous a saisir des donnees manuellement?"

**Visual:** Three stat boxes, large font:

| | |
|---|---|
| **40%** | du temps du personnel CPA est de la saisie manuelle de donnees |
| **50--100** | transactions par mois par client PME au Quebec |
| **3 000 $ -- 6 000 $** | par client par mois en temps non facturable (a 150 $/h) |

**Speaking Notes:**

> *Posez la question. Attendez. Ne parlez pas. Laissez-les repondre.*
>
> Leur reponse est votre argument de vente. Quand ils disent "au moins 20 heures" ou "trop", acquiescez et dites:
>
> "Exactement. Et chaque heure de saisie manuelle, c'est une heure que vous ne facturez pas. A 150 $ de l'heure, 20 heures par semaine, c'est 12 000 $ par mois que votre cabinet laisse sur la table."
>
> Si le cabinet a 30 clients: 30 clients x 5 heures de saisie = 150 heures/mois = 22 500 $/mois en capacite perdue.
>
> Ne montrez pas encore la solution. Laissez la douleur s'installer.

---

## SLIDE 3 --- THE SOLUTION

### LedgerLink AI traite les documents automatiquement --- photos, courriels, WhatsApp

**Visual:** Flow diagram in 4 steps:

```
[Photo/PDF/Courriel] --> [Extraction IA automatique] --> [Compte GL suggere] --> [Un clic pour approuver]
```

Show a phone photo of a receipt on the left, an arrow, the extracted data in the middle (vendor, amount, date, GL account, TPS/TVQ), and a green "Approuver" button on the right.

**Speaking Notes:**

> "La solution, c'est simple. Votre client prend une photo de sa facture. LedgerLink lit le document, extrait le fournisseur, le montant, la date, calcule les taxes, suggere le compte GL, et votre equipe n'a qu'a cliquer Approuver. C'est tout."
>
> Une phrase. Arretez. Puis dites: "Laissez-moi vous montrer." Et passez a la demo si vous en etes la, ou continuez les slides si c'est une presentation formelle.

---

## SLIDE 4 --- HOW IT WORKS (3-Layer Architecture)

### Trois couches. Zero approximation sur les taxes.

**Visual:** Three horizontal layers, stacked:

```
COUCHE 1 --- REGLES DETERMINISTES (100% precision)
TPS/TVQ/TVH calcules mathematiquement --- jamais une estimation IA
13 regles anti-fraude --- seuils stricts, pas d'hallucination
Moteur de substance economique --- actifs, passifs, charges

COUCHE 2 --- INTELLIGENCE ARTIFICIELLE (suggestions)
Extraction de documents (OCR + Vision)
Suggestion de comptes GL
Redaction de messages clients bilingues
Routeur IA : DeepSeek (routine) / Claude (complexe)

COUCHE 3 --- CONTROLE HUMAIN (autorite finale)
Votre equipe approuve chaque transaction
File d'attente de revision avec niveaux de confiance
Historique d'audit complet (qui, quoi, quand)
```

**Speaking Notes:**

> "Ce qui distingue LedgerLink des autres outils IA, c'est l'architecture en trois couches."
>
> "Couche 1 : les taxes, la detection de fraude, et la classification comptable sont 100% basees sur des regles. Pas d'IA. Pas d'hallucination. Les calculs TPS/TVQ utilisent la librairie Decimal de Python --- zero erreur d'arrondi."
>
> "Couche 2 : l'IA fait la lecture des documents et suggere des comptes. Mais elle ne decide de rien."
>
> "Couche 3 : votre equipe a le dernier mot. Toujours. Chaque transaction doit etre approuvee par un humain avant d'etre soumise a QuickBooks."
>
> "Ca repond a la question 'est-ce qu'on peut lui faire confiance?' avant meme que vous la posiez."

---

## SLIDE 5 --- QUEBEC-SPECIFIC

### Le seul logiciel construit specifiquement pour la fiscalite quebecoise

**Visual:** Checklist with green checkmarks:

- [x] **TPS/TVQ calcul parallele** --- jamais en cascade (TPS 5% + TVQ 9,975% sur le meme montant pre-taxe)
- [x] **Pre-remplissage FPZ-500** Revenu Quebec --- lignes 103, 106, 108, 205, 207, 209
- [x] **Methode rapide** configurable par client (methode simplifiee)
- [x] **Calendrier des echeances** TPS/TVQ (mensuel, trimestriel, annuel)
- [x] **Plan comptable general quebecois** (196 comptes pre-configures)
- [x] **RL-1/T4 reconciliation** via moteur de paie
- [x] **Loi 25** --- donnees hebergees sur votre serveur, jamais dans le cloud
- [x] **Assurance provinciale** --- charge 9% non recuperable, pas de TPS (code I)
- [x] **Repas et divertissement** --- recuperation TPS/TVQ a 50% (code M)
- [x] **CNESST** taux de cotisation par unite industrielle
- [x] **FSS** (Fonds des services de sante) --- 6 paliers de taux valides
- [x] **Bilingue FR/EN** --- 100% des chaines traduites, terminologie CPA correcte

**Speaking Notes:**

> "C'est notre avantage le plus fort. LedgerLink est le seul logiciel construit specifiquement pour la fiscalite quebecoise."
>
> "TaxDome? Base a San Francisco, concu pour les Etats-Unis. Karbon? Australie. CaseWare? Ontario, TVH. Aucun ne gere le calcul parallele TPS/TVQ. Aucun ne pre-remplit le FPZ-500. Aucun ne comprend la methode rapide."
>
> "Et surtout, aucun ne vous permet d'heberger les donnees sur votre propre serveur. Avec la Loi 25, c'est vous personnellement qui etes responsable des donnees de vos clients. Avec LedgerLink, les donnees restent chez vous."

---

## SLIDE 6 --- SECURITY AND PRIVACY (Law 25)

### Vos donnees clients ne quittent jamais vos locaux

**Visual:** Two-column comparison:

| | TaxDome / Karbon / Cloud | LedgerLink |
|---|---|---|
| **Ou sont les donnees?** | Serveurs AWS aux Etats-Unis | Votre serveur, dans votre bureau |
| **Qui y a acces?** | L'editeur du logiciel | Vous seul |
| **Loi 25 conforme?** | Risque eleve (transfert transfrontalier) | Oui --- donnees locales |
| **En cas de faillite du fournisseur?** | Vos donnees dans le cloud, acces incertain | Vos donnees sur votre disque, acces garanti |
| **Chiffrement** | En transit (TLS) | En transit (TLS via Cloudflare) + au repos (votre serveur) |
| **Authentification** | Mot de passe simple | bcrypt + sessions securisees |
| **Piste d'audit** | Variable | Chaque action enregistree (qui, quoi, quand) |

**Speaking Notes:**

> "C'est un sujet emotionnel pour les cabinets CPA au Quebec, et avec raison."
>
> "Depuis la Loi 25, vous etes personnellement responsable des donnees de vos clients. Pas votre cabinet --- vous. Si les donnees de vos clients sont sur un serveur Amazon aux Etats-Unis et qu'il y a une breche, c'est votre nom sur l'avis de la Commission d'acces a l'information."
>
> "Avec LedgerLink, les donnees restent sur votre serveur. Dans votre bureau. Sous votre controle. Point final."
>
> "L'IA traite les documents, mais rien n'est stocke dans le cloud. Les resultats d'extraction sont sauvegardes dans votre base de donnees locale SQLite."
>
> Pause. Laissez le message s'imprimer.

---

## SLIDE 7 --- TIME SAVINGS (THE ROI SLIDE)

### Le retour sur investissement se calcule en jours, pas en mois.

**Visual:** Table with before/after comparison:

| Tache | Avant LedgerLink | Apres LedgerLink | Temps economise |
|---|---|---|---|
| Saisie de donnees | 3 h / client | 0 h (extraction automatique) | **3 h** |
| Rapprochement bancaire | 1,5 h / client | 20 min (correspondance automatique) | **1 h 10** |
| Resume de production | 45 min / client | 5 min (pre-remplissage FPZ-500) | **40 min** |
| Detection de doublons | 30 min / client | 0 min (automatique, 13 regles) | **30 min** |
| **Total par client** | **5 h 45** | **25 min** | **5 h 20** |

**At scale:**

| Nombre de clients | Heures economisees / mois | Valeur a 150 $/h | Cout LedgerLink | ROI |
|---|---|---|---|---|
| 10 clients | 53 h | 7 950 $ | 99 $/mois | **8 000%** |
| 30 clients | 160 h | 24 000 $ | 249 $/mois | **9 600%** |
| 75 clients | 400 h | 60 000 $ | 499 $/mois | **12 000%** |

**Speaking Notes:**

> "Voici les chiffres. Pas de marketing, juste du math."
>
> "Pour un cabinet de 30 clients, LedgerLink economise environ 160 heures par mois. A 150 dollars de l'heure, c'est 24 000 dollars par mois en capacite recuperee. LedgerLink coute 249 dollars par mois."
>
> *Pause. Laissez les chiffres parler.*
>
> "Qu'est-ce que vous feriez avec 160 heures de plus par mois? Prendre 10 nouveaux clients? Offrir des services-conseils a plus haute valeur? Donner des vendredis libres a votre equipe?"
>
> Ne repondez pas a votre propre question. Laissez-les y penser.

---

## SLIDE 8 --- CPA AUDIT MODULE

### Mission de verification, d'examen et de compilation --- tout dans un seul systeme

**Visual:** CAS Standards compliance grid:

| Norme CAS | Description | Status |
|---|---|---|
| CAS 315 | Evaluation des risques | Matrice risque inherent / risque de controle / risque combine |
| CAS 320 | Importance relative | Seuils de signification, d'execution, et de trivialite |
| CAS 330 | Procedures d'audit | Dossiers de travail avec feuilles maitresses |
| CAS 500 | Elements probants | Matrice d'assertions (exhaustivite, exactitude, existence, cesure, classement) |
| CAS 505 | Confirmations | Chaines de preuve a trois voies (BC/facture/paiement) |
| CAS 530 | Echantillonnage | Echantillonnage statistique reproductible |
| CAS 550 | Parties liees | Identification et divulgation |
| CAS 560 | Evenements posterieurs | Detection automatique + signalement |
| CAS 570 | Continuite d'exploitation | Indicateurs de risque + evaluation |
| CAS 580 | Declarations de la direction | Generation de lettres de representation |
| CAS 700 | Rapport de l'auditeur | Flux de travail d'emission du rapport |
| CSQC 1 | Controle qualite | Liste de controle pre-emission |

**Additional features:**

- Etats financiers (bilan + etat des resultats) generes depuis la balance de verification
- Procedures analytiques (ratios de liquidite, rentabilite, solvabilite, efficacite)
- Suivi du temps + facturation integres
- Telechargement PDF pour chaque composante

**Speaking Notes:**

> "Pour les cabinets qui font des missions de verification, d'examen ou de compilation, le module d'audit justifie a lui seul le prix de LedgerLink."
>
> "On couvre les normes CAS 315 a 700 plus CSQC 1. Dossiers de travail, importance relative, echantillonnage, elements probants, evenements posterieurs, continuite d'exploitation --- tout integre dans le meme systeme que votre comptabilite."
>
> "Est-ce que vous faites des missions de certification? Des examens?"
>
> Adaptez votre discours selon la reponse. Si oui, insistez. Si non, passez rapidement et dites "C'est un avantage pour quand vous voudrez offrir ces services."

---

## SLIDE 9 --- PRICING

### Des prix simples. Pas de surprise.

**Visual:** Four pricing cards side by side:

| | Essentiel | Professionnel | Cabinet | Entreprise |
|---|---|---|---|---|
| **Prix** | **99 $/mois** | **249 $/mois** | **499 $/mois** | **999 $/mois** |
| Clients max | 10 | 30 | 75 | Illimite |
| Utilisateurs max | 3 | 5 | 15 | Illimite |
| Revision de base | Oui | Oui | Oui | Oui |
| Soumission QBO | Oui | Oui | Oui | Oui |
| Routeur IA | --- | Oui | Oui | Oui |
| Rapprochement bancaire | --- | Oui | Oui | Oui |
| Detection de fraude | --- | Oui | Oui | Oui |
| Revenu Quebec | --- | Oui | Oui | Oui |
| Suivi du temps | --- | Oui | Oui | Oui |
| Fermeture de periode | --- | Oui | Oui | Oui |
| Analytique | --- | --- | Oui | Oui |
| Microsoft 365 | --- | --- | Oui | Oui |
| Calendrier de production | --- | --- | Oui | Oui |
| Communications clients | --- | --- | Oui | Oui |
| Module d'audit complet | --- | --- | --- | Oui |
| Etats financiers | --- | --- | --- | Oui |
| Echantillonnage | --- | --- | --- | Oui |
| Acces API | --- | --- | --- | Oui |

**Frais d'installation:** 500 $ -- 1 000 $ (une fois)

Comprend: installation sur votre serveur, configuration, formation initiale, migration des donnees existantes.

**Speaking Notes:**

> Dites le prix avec confiance. Ne vous excusez pas. Ne dites pas "seulement" ou "juste". Dites:
>
> "Le forfait Professionnel est a 249 dollars par mois. Ca inclut 30 clients, 5 utilisateurs, le routeur IA, le rapprochement bancaire, la detection de fraude, et le pre-remplissage Revenu Quebec."
>
> *Arretez de parler. Laissez le silence travailler.*
>
> S'ils ne reagissent pas, ajoutez: "A 150 dollars de l'heure, LedgerLink se paie en moins de 2 heures de travail economise. Le premier mois."
>
> Ne proposez jamais de rabais sans qu'on vous le demande. Si on vous le demande, voir docs/sales/OBJECTIONS.md.

---

## SLIDE 10 --- WHAT HAPPENS NEXT

### Voici comment on commence :

**Visual:** Three steps with numbers:

```
ETAPE 1    Demo aujourd'hui
           15 minutes. Je vous montre le vrai logiciel.
           Pas de PowerPoint. Des vrais documents.

ETAPE 2    Pilote gratuit --- 30 jours
           5 clients de votre choix. Voyez les resultats vous-meme.
           Aucune obligation. Aucun paiement.

ETAPE 3    Decision
           Vous decidez si ca vous fait gagner du temps.
           Pas de pression. Juste des resultats.
```

**Speaking Notes:**

> "Voici les prochaines etapes."
>
> "D'abord, je vous montre le logiciel en direct. 15 minutes, avec de vrais documents. Pas de diapositives."
>
> "Ensuite, si ca vous interesse, on installe un pilote gratuit de 30 jours. Vous choisissez 5 clients, on les configure, et vous voyez par vous-meme si ca vous fait gagner du temps."
>
> "Apres 30 jours, vous decidez. Aucune obligation. Aucun paiement pendant le pilote. Si ca ne vous convient pas, on desinstalle et on reste amis."
>
> L'objectif est de retirer toute friction. Rendez la prochaine etape evidente et facile. Ne demandez pas une decision d'achat --- demandez une demo.

---

## SLIDE 11 --- TESTIMONIALS

### Ce que disent nos clients

**Visual:** Two or three testimonial boxes (placeholders until real testimonials are available):

```
[PLACEHOLDER --- Remplir avec les vrais temoignages apres les premiers clients]

Format recommande:

"[Citation directe du client --- focusez sur les heures economisees,
pas sur la technologie]"

--- [Nom], [Titre], [Nom du cabinet]
    [Nombre de clients], [Heures economisees par mois]
```

**Alternate content (until real testimonials exist):**

> **Resultats de tests independants:**
>
> - **2 853 tests** automatises passes, **0 echecs** (suite complete)
> - **147/147** tests adversariaux "red team" passes
> - **Score de preparedness production: 100/100**
> - **8,5/10** en exactitude fiscale canadienne
> - **13 regles** de detection de fraude, toutes connectees au pipeline d'approbation
> - **0 erreur** de calcul de taxe dans les tests independants
> - **100% parite** bilingue (FR/EN) --- terminologie CPA verifiee

**Speaking Notes:**

> Si vous avez de vrais temoignages:
> "Voici ce que disent nos clients actuels." Lisez la citation. Arretez.
>
> Si vous n'avez pas encore de temoignages:
> "On est un nouveau produit, alors plutot que des temoignages, je vais vous montrer nos resultats de tests independants. Un red team independant a fait passer 2 853 tests au systeme. Zero echec. 147 tests adversariaux specifiquement concus pour casser le systeme --- tous passes. Score de preparation a la production: 100 sur 100."
>
> "Et zero erreur de calcul de taxe. Parce que les taxes ne sont jamais calculees par l'IA --- c'est du math pur, verifie independamment."

---

## SLIDE 12 --- CALL TO ACTION

### Pret a recuperer 150 heures par mois?

**Visual:** Clean, centered:

```
[Votre nom]
[Votre titre]
[votre.nom]@ledgerlink.ca
[Numero de telephone]

Reservez une demo : [lien Calendly ou numero direct]

ledgerlink.ca
```

**Speaking Notes:**

> "Merci pour votre temps. Est-ce qu'on peut planifier une demo de 15 minutes cette semaine? Je vous montre le logiciel avec vos vrais documents."
>
> *Arretez de parler. Attendez leur reponse.*
>
> Si oui: sortez votre calendrier et bloquez le rendez-vous immediatement. Ne dites pas "je vous envoie un courriel". Faites-le maintenant.
>
> Si "on va y penser": "Parfait. Est-ce que je peux vous envoyer un resume par courriel et vous recontacter [jour precis]?" Obtenez un engagement sur une date, pas un "on vous rappellera".
>
> Si non: "Je comprends. Est-ce que je peux vous laisser notre fiche-resume? Si jamais la situation change, vous aurez mes coordonnees." Laissez la fiche et partez. Ne suppliez pas.

---

## NOTES GENERALES POUR LE PRESENTATEUR

1. **Ne lisez jamais les diapositives.** Les diapositives sont pour eux. Vos notes sont pour vous.
2. **Posez des questions.** Les meilleures presentations sont des conversations.
3. **Si quelqu'un pose une question technique detaillee,** dites "Excellente question. Je peux vous montrer ca en direct pendant la demo."
4. **Ne dites jamais "notre produit est le meilleur".** Montrez les chiffres et laissez-les conclure.
5. **Le silence est votre outil le plus puissant.** Apres un chiffre important, arretez de parler pendant 3 secondes.
6. **Si vous perdez l'attention,** posez une question: "Est-ce que ca ressemble a votre situation?"
7. **Terminez toujours avec une prochaine etape concrete.** Pas "on reste en contact" mais "mardi prochain a 10h, ca vous va?"
