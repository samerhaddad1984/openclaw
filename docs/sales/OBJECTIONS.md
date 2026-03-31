# OtoCPA --- Objection Handling Guide

**Rule #1:** Every objection is a question in disguise. Find the question. Answer it.
**Rule #2:** Never argue. Acknowledge, reframe, respond.
**Rule #3:** After your response, ask a question. Move the conversation forward.

---

## "On utilise deja TaxDome / Karbon / CaseWare"

### What they're really saying:
"I already spent money and time on something. Convince me it's worth switching."

### Response:

> "Excellent. Comment ca se passe avec [leur outil]? Est-ce que la saisie de factures est vraiment automatisee, ou c'est plus un systeme de gestion de taches?"

*Wait for their answer. They'll usually admit the data entry is still manual.*

> "C'est ce qu'on entend souvent. [TaxDome/Karbon/CaseWare] est bon pour la gestion de projet et la communication. Mais aucun d'entre eux ne fait l'extraction automatique de factures, le calcul TPS/TVQ parallele, ou le pre-remplissage FPZ-500."
>
> "OtoCPA ne remplace pas [leur outil]. Il s'ajoute en amont. [Leur outil] gere le flux de travail. OtoCPA elimine la saisie. Les deux peuvent coexister."
>
> "Est-ce que ca vaudrait la peine de voir comment les deux fonctionnent ensemble?"

### Key differentiators to mention:

| Feature | TaxDome | Karbon | CaseWare | OtoCPA |
|---|---|---|---|---|
| Extraction automatique IA | Non | Non | Non | Oui |
| Calcul TPS/TVQ parallele | Non | Non | Partiel | Oui |
| Pre-remplissage FPZ-500 | Non | Non | Non | Oui |
| Donnees locales (Loi 25) | Non (cloud US) | Non (cloud AU) | Partiel | Oui |
| Detection de fraude | Non | Non | Partiel | 13 regles |
| Module d'audit CAS | Non | Non | Oui | Oui |
| Bilingue FR/EN natif | Partiel | Non | Partiel | 100% |

---

## "C'est trop cher"

### What they're really saying:
"I don't see enough value to justify the price." or "I need to justify this to my partner."

### Response:

> "Je comprends. Regardons les chiffres ensemble."
>
> "Combien de clients actifs avez-vous?" *[Ils repondent, disons 25]*
>
> "Et combien d'heures de saisie par client par mois, approximativement?" *[Disons 4 heures]*
>
> "OK. 25 clients fois 4 heures, c'est 100 heures par mois de saisie. A 150 dollars de l'heure --- meme si c'est du temps non facture, c'est la capacite que ca represente --- c'est 15 000 dollars par mois."
>
> "OtoCPA Professionnel coute 249 dollars par mois. Ca represente 1,7% du temps que vous recuperez."
>
> "Autrement dit: est-ce que le logiciel va vous faire economiser plus de 2 heures par mois? Si oui, il est deja rentable."

*Stop talking. Let the math work.*

### If they push back further:

> "Je comprends qu'il y a un investissement. C'est pour ca qu'on offre un pilote de 30 jours gratuit. Vous testez avec 5 clients, vous mesurez les heures economisees, et vous decidez avec des vrais chiffres. Aucun risque."

---

## "On n'a pas le temps d'implanter un nouveau logiciel"

### What they're really saying:
"I'm already overwhelmed. Adding one more thing feels impossible."

### Response:

> "C'est justement pour ca que OtoCPA existe --- pour vous redonner du temps, pas vous en enlever."
>
> "L'installation prend 30 minutes. On fait tout a distance --- on installe sur votre serveur, on configure vos 5 premiers clients, et votre equipe peut commencer a l'utiliser le jour meme."
>
> "Il n'y a rien a apprendre de complique. L'interface ressemble a une file d'attente de courriels. Document entre, votre equipe revise, un clic pour approuver."
>
> "Est-ce que 30 minutes cette semaine, ca serait faisable? On fait l'installation pendant que vous continuez a travailler."

### Key point:
Frame the implementation as something **you** do, not something **they** do. Minimize their effort.

---

## "Nos clients ne l'utiliseront pas"

### What they're really saying:
"I'm worried about change management on the client side."

### Response:

> "En fait, vos clients n'ont rien a apprendre. Le seul changement pour eux, c'est de prendre une photo de leur facture au lieu de la scanner ou de la mettre dans une enveloppe."
>
> "On genere un code QR par client. Le client scanne le code avec son telephone, prend une photo, et c'est envoye. Pas d'application a installer. Pas de mot de passe a retenir."
>
> "Et pour les clients qui ne veulent vraiment pas changer: pas de probleme. Ils continuent a envoyer par courriel ou par la poste. OtoCPA traite les courriels et les PDF de la meme facon."
>
> "Est-ce que vous avez des clients plus technos qui pourraient etre de bons candidats pour le pilote?"

---

## "Qu'est-ce qui arrive a nos donnees si vous fermez?"

### What they're really saying:
"I don't trust startups. What if you disappear?"

### Response:

> "Excellente question. C'est l'avantage principal de OtoCPA par rapport aux solutions cloud."
>
> "Toutes vos donnees sont sur votre serveur. Votre base de donnees SQLite, vos documents, votre configuration --- tout est dans un dossier sur votre machine. Si OtoCPA disparait demain, vos donnees sont toujours la. Vous pouvez les lire avec n'importe quel outil SQLite gratuit."
>
> "Comparez ca a TaxDome ou Karbon. Si eux ferment, vos donnees sont sur leurs serveurs Amazon. Bonne chance pour les recuperer."
>
> "Et on fait des sauvegardes automatiques quotidiennes. Vous pouvez telecharger une copie de votre base de donnees a n'importe quel moment depuis le tableau de bord."
>
> "Est-ce que la question des donnees est votre principale preoccupation, ou il y a autre chose?"

---

## "On va y penser"

### What they're really saying:
"I'm not ready to decide, and I don't want to say no to your face."

### Response:

> "Bien sur, c'est une decision importante. Est-ce que je peux vous poser une question?"
>
> *Attendez qu'ils disent oui.*
>
> "Qu'est-ce qui vous ferait pencher d'un cote ou de l'autre?"

*This question reveals the real objection. Listen carefully. Then address it.*

### If they can't articulate the objection:

> "Est-ce que c'est le prix, le timing, ou la confiance dans la technologie?"

*Give them three options. They'll pick one. Now you know what to address.*

### After addressing the real objection:

> "Est-ce que ca repond a votre question? Si oui, est-ce qu'on peut planifier le pilote de 30 jours pour [date precise]?"

### If they truly need time:

> "Pas de probleme. Est-ce que je peux vous recontacter [jour precis]? Comme ca, vous avez le temps d'y reflechir sans que ca tombe dans l'oubli."

*Get a specific date. "La semaine prochaine" is not a date. "Mardi a 14h" is a date.*

---

## "Est-ce qu'on peut avoir un rabais?"

### What they're really saying:
"I want to buy, but I want to feel like I got a deal." (This is a buying signal.)

### Response (Option A --- hold the price):

> "Le prix est le meme pour tous nos clients --- c'est important pour nous d'etre equitables. Ce qu'on peut faire par contre, c'est inclure la configuration de 5 clients additionnels dans les frais d'installation. Ca vous donnerait 10 clients configures au lieu de 5 pour le meme prix."

### Response (Option B --- annual commitment):

> "Si vous vous engagez pour 12 mois, on peut inclure l'installation sans frais supplementaires. Ca represente une economie de 500 a 1 000 dollars."

### Response (Option C --- pilot leverage):

> "Le pilote de 30 jours est gratuit. Si apres le pilote vous decidez de continuer, on peut appliquer le premier mois a 50% comme remerciement d'etre un client fondateur."

### Rules:

- **Never discount the monthly price.** It sets a precedent.
- **Add value instead.** More clients configured, free installation, extra training.
- **If they insist on a lower monthly price:** "Le prix reflete la valeur. A 249 $/mois, vous recuperez au moins 15 000 $ de capacite. Un rabais de 20 $ ne change pas cette equation."

---

## "Qu'est-ce qui arrive si l'IA fait une erreur?"

### What they're really saying:
"I don't trust AI. I'm scared it will mess up my clients' books."

### Response:

> "C'est la bonne question, et c'est pour ca qu'on a construit OtoCPA en trois couches."
>
> "Les taxes --- TPS, TVQ, TVH --- ne sont jamais calculees par l'IA. C'est du math pur, base sur des regles. On utilise la librairie Decimal de Python --- zero erreur d'arrondi. Un red team independant a teste 98 scenarios de taxes. Zero erreur."
>
> "L'IA fait la lecture des documents et suggere des comptes GL. Mais elle ne decide de rien. Chaque transaction doit etre approuvee par un humain avant d'etre soumise a QuickBooks."
>
> "Et si l'IA suggere quelque chose de bizarre, le systeme le detecte et le met en file de revision. Votre equipe le voit, le corrige, et le systeme apprend de la correction."
>
> "En resume: l'IA peut suggerer une erreur. Mais elle ne peut jamais la publier sans l'approbation de votre equipe."

---

## "Est-ce que c'est approuve par l'Ordre des CPA?"

### What they're really saying:
"Am I allowed to use this? Will I get in trouble?"

### Response:

> "L'Ordre des CPA n'approuve pas de logiciels specifiques --- ni QuickBooks, ni CaseWare, ni aucun autre. Ce qu'ils exigent, c'est que le CPA exerce son jugement professionnel et maintienne une piste d'audit adequate."
>
> "OtoCPA est concu exactement pour ca. Chaque transaction est revue par un humain. Chaque modification est enregistree dans une piste d'audit complete --- qui a fait quoi, quand. Les taxes sont calculees de facon deterministe, pas par IA."
>
> "C'est comme utiliser une calculatrice --- l'outil fait le calcul, le CPA valide le resultat. L'Ordre n'a rien a redire la-dessus."
>
> "Est-ce qu'il y a un aspect specifique de la conformite qui vous preoccupe?"

---

## "On est trop petit" / "On est trop gros"

### "On est trop petit" (fewer than 10 clients):

> "Le forfait Essentiel est concu exactement pour les cabinets solo ou les petites equipes. 99 dollars par mois pour 10 clients et 3 utilisateurs."
>
> "A 99 dollars, si le logiciel vous fait economiser plus d'une heure par mois --- une seule heure --- il est deja rentable."
>
> "Et quand vous grandissez, vous passez au forfait Professionnel sans rien reinstaller. C'est juste un changement de cle de licence."

### "On est trop gros" (more than 75 clients):

> "Le forfait Entreprise est illimite --- clients, utilisateurs, tout. A 999 dollars par mois."
>
> "Pour un cabinet de votre taille, 999 dollars represente probablement moins d'une heure de facturation. Et vous recuperez des centaines d'heures par mois."
>
> "En plus, le forfait Entreprise inclut le module d'audit complet avec les normes CAS, les etats financiers automatises, et l'echantillonnage statistique. Pour un cabinet qui fait des missions de certification, ca vaut le prix a lui seul."
>
> "Est-ce que vous faites des missions d'audit ou d'examen?"

---

## "On vient de renouveler notre contrat avec [concurrent]"

### What they're really saying:
"I can't switch right now, but I might be interested later."

### Response:

> "Pas de probleme. OtoCPA ne remplace pas [concurrent] --- il s'ajoute en amont pour automatiser la saisie."
>
> "Mais si vous voulez attendre la fin de votre contrat, quand est-ce qu'il se termine?"

*Note the date.*

> "Parfait. Est-ce que je peux vous recontacter [1 mois avant la fin du contrat]? Comme ca, vous aurez le temps de comparer avant de renouveler."
>
> "En attendant, est-ce qu'un pilote gratuit de 30 jours vous interesserait? Ca ne touche pas a votre contrat actuel, et ca vous donnerait des vrais chiffres pour comparer."

### Rules:

- **Never badmouth the competitor.** It looks desperate.
- **Note the contract end date in your CRM.** Set a reminder 30 days before.
- **Offer the pilot anyway.** It's free, it doesn't conflict, and it creates momentum.

---

## MASTER FRAMEWORK: THE ACKNOWLEDGE-REFRAME-RESPOND METHOD

For any objection not listed above:

1. **Acknowledge:** "Je comprends." / "C'est une bonne question." / "Vous avez raison de demander."
2. **Reframe:** Turn the objection into a question. "Ce que vous demandez vraiment, c'est..."
3. **Respond:** Answer the reframed question with facts, numbers, or a story.
4. **Ask:** Move forward. "Est-ce que ca repond a votre question? Si oui, [prochaine etape]."

### Example:

**Objection:** "L'IA, ca me fait peur."

1. **Acknowledge:** "Je comprends tout a fait. Beaucoup de nos clients avaient la meme reaction au debut."
2. **Reframe:** "Ce que vous demandez vraiment, c'est: est-ce que l'IA peut publier une erreur sans que je le sache?"
3. **Respond:** "Non. Chaque transaction passe par une file de revision. Rien ne va dans QuickBooks sans l'approbation d'un humain. L'IA suggere, votre equipe decide."
4. **Ask:** "Est-ce que ca vous rassure? Si oui, est-ce qu'on fait un pilote de 30 jours pour que vous voyiez par vous-meme?"
